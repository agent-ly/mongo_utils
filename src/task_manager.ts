import { EventEmitter } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import { randomUUID } from "node:crypto";
import type { Collection, InferIdType, WithId } from "mongodb";

// Public classes

export class TaskManager extends EventEmitter {
  static readonly #DEFAULT_TASK_CONCURRENCY = 5;
  static readonly #DEFAULT_TASK_LOCK_TIMEOUT = 1e3 * 30;
  static readonly #DEFAULT_TASK_PROLONG_LOCK_TIMEOUT = 1e3 * 15;
  static readonly #DEFAULT_DRAIN_TIMEOUT = 1e3 * 5;

  readonly #collection: Collection<Task>;
  readonly #options: TaskManagerOptions;
  readonly #id = randomUUID();
  readonly #singleTaskFns = new Map<string, SingleTaskFn>();
  readonly #repeatingTaskFns = new Map<string, RepeatingTaskFn>();
  readonly #queue = new Map<Promise<WithId<Task> | null>, string>();
  #stopping: Promise<void> | null = null;
  #running = false;
  #waiting = false;
  #drained = false;
  #counter = 0;

  // Public methods

  constructor(
    collection: Collection,
    {
      concurrency = TaskManager.#DEFAULT_TASK_CONCURRENCY,
      lockTimeout = TaskManager.#DEFAULT_TASK_LOCK_TIMEOUT,
      prolongLockTimeout = TaskManager.#DEFAULT_TASK_PROLONG_LOCK_TIMEOUT,
      drainTimeout = TaskManager.#DEFAULT_DRAIN_TIMEOUT,
      removeFinishedTasks = true,
      removeFailedTasks = false,
    }: Partial<TaskManagerOptions> = {}
  ) {
    super();
    this.#collection = collection as unknown as Collection<Task>;
    this.#options = {
      concurrency,
      lockTimeout,
      prolongLockTimeout,
      drainTimeout,
      removeFinishedTasks,
      removeFailedTasks,
    };
  }

  async createTaskIndexes() {
    await this.#collection.createIndex({ startsAt: 1, lockedAt: 1 });
    await this.#collection.createIndex({ _id: 1, lockedAt: 1 });
  }

  async addSingleTask(options: AddSingleTaskOptions) {
    if (!this.#singleTaskFns.has(options.name))
      throw new Error(`Task ${options.name} not defined`);
    const now = new Date();
    const task: Task = {
      kind: "single",
      name: options.name,
      data: options.data ?? null,
      progress: null,
      error: null,
      startsAt: options.startsAt,
      finishedAt: null,
      failedAt: null,
      lockedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    const { insertedId } = await this.#collection.insertOne(task);
    return insertedId;
  }

  async addRepeatingTask(options: AddRepeatingTaskOptions) {
    if (!this.#repeatingTaskFns.has(options.name))
      throw new Error(`Task ${options.name} not defined`);
    const now = new Date();
    const nextRunAt = getNextRunAt(options.repeatsEvery, now.getTime());
    const task: Task = {
      kind: "repeating",
      name: options.name,
      runs: 0,
      repeatsEvery: options.repeatsEvery,
      progress: null,
      error: null,
      startsAt: nextRunAt,
      lastFinishedAt: null,
      lastFailedAt: null,
      lockedAt: null,
      createdAt: now,
      updatedAt: now,
    };
    const exists = await this.#collection.findOne({ name: options.name });
    if (exists) {
      // We don't use upsert because we want to maintain the positions of each field
      await this.#collection.updateOne({ _id: exists._id }, { $set: task });
      return exists._id;
    } else {
      const { insertedId } = await this.#collection.insertOne(task);
      return insertedId;
    }
  }

  startTasks() {
    if (!this.#collection)
      throw new Error("Task Manager not initialized with collection");
    return this.#startQueue();
  }

  stopTasks() {
    return this.#stopQueue();
  }

  defineTask(name: string, kind: "repeating", fn: RepeatingTaskFn): void;
  defineTask<TData>(
    name: string,
    kind: "single",
    fn: SingleTaskFn<TData>
  ): void;
  defineTask(
    name: string,
    kind: "single" | "repeating",
    fn: SingleTaskFn | RepeatingTaskFn
  ) {
    const fns =
      kind === "single" ? this.#singleTaskFns : this.#repeatingTaskFns;
    if (fns.has(name))
      throw new Error(`Task ${name} already defined as ${kind}`);
    fns.set(name, fn as SingleTaskFn & RepeatingTaskFn);
  }

  findTaskById(taskId: InferIdType<Task>) {
    return this.#collection.findOne({ _id: taskId });
  }

  countTasks() {
    return this.#collection.countDocuments();
  }

  clearTasks(includeRepeating = false) {
    return this.#collection.deleteMany(
      includeRepeating ? {} : { kind: "single" }
    );
  }

  // Private methods

  async #stopQueue() {
    if (!this.#stopping) {
      this.#stopping = new Promise(async (resolve) => {
        this.emit("queue.stopping");
        await Promise.all([...this.#queue.keys()])
          .catch((error) => this.emit("queue.error", error))
          .finally(() => this.emit("queue.stopped"));
        resolve();
      });
    }
    return this.#stopping;
  }

  async #startQueue() {
    if (this.#running) return;
    this.#running = true;
    this.#stopping = null;
    this.emit("queue.starting");
    while (this.#stopping === null) {
      this.#fillQueue();
      await this.#processQueue();
    }
    this.#running = false;
  }

  #fillQueue() {
    if (!this.#waiting && this.#queue.size < this.#options.concurrency) {
      const token = `${this.#id}-${this.#counter++}`;
      this.#queue.set(this.#getNextTask(), token);
    }
  }

  async #processQueue() {
    const promises = [...this.#queue.keys()];
    const index = await Promise.race(promises.map((p, i) => p.then(() => i)));
    const promise = promises[index];
    const task = (await promise) as WithId<Task> | null;
    if (task) {
      const token = this.#queue.get(promise)!;
      const processed = this.#processTask(task).then(() => {
        if (this.#queue.size <= this.#options.concurrency)
          return this.#findNextTask();
        return null;
      });
      this.#queue.set(processed, token);
    }
    this.#queue.delete(promise);
  }

  async #getNextTask() {
    if (this.#stopping) return null;
    if (this.#drained) {
      this.#waiting = true;
      await sleep(this.#options.drainTimeout);
    }
    const task = await this.#findNextTask();
    this.#waiting = false;
    return task;
  }

  async #findNextTask() {
    const now = new Date();
    // Find and lock next task
    const task = await this.#collection.findOne({
      $and: [
        { startsAt: { $lte: now } },
        {
          $or: [
            { lockedAt: { $eq: null } },
            {
              lockedAt: {
                $lte: new Date(now.getTime() - this.#options.lockTimeout),
              },
            },
          ],
        },
      ],
    });
    if (!task) {
      if (!this.#drained) {
        this.#drained = true;
        this.emit("queue.drained");
      }
      return null;
    } else {
      this.#drained = false;
      return task;
    }
  }

  async #processTask(task: WithId<Task>) {
    if (!(await this.#lockTask(task))) return null;

    let success = true;
    let repeats: boolean | void = true;
    const progress = async (progress?: string | number) => {
      const isDefined = progress !== undefined;
      if (isDefined) task.progress = progress;
      task.lockedAt = new Date();
      task.updatedAt = new Date();
      await this.#saveTask(task);
      if (isDefined) this.emit("task.progress", task);
    };

    try {
      if (task.kind === "single") {
        const fn = this.#singleTaskFns.get(task.name)!;
        await fn({ progress, data: task.data });
        task.finishedAt = new Date();
      } else if (task.kind === "repeating") {
        const fn = this.#repeatingTaskFns.get(task.name)!;
        task.runs++;
        repeats = await fn({ progress });
        task.lastFinishedAt = new Date();
      }
    } catch (error) {
      task.error = (error as Error).message;
      if (task.kind === "single") task.failedAt = new Date();
      else if (task.kind === "repeating") task.lastFailedAt = new Date();
      success = false;
    } finally {
      task.lockedAt = null;
      task.startsAt =
        task.kind === "repeating" && repeats !== false
          ? getNextRunAt(task.repeatsEvery)
          : null;
      task.updatedAt = new Date();
    }

    const event = success ? "task.finished" : "task.failed";
    this.emit(event, task);
    if (
      task.kind === "single" &&
      ((!success && this.#options.removeFailedTasks) ||
        (success && this.#options.removeFinishedTasks))
    ) {
      await this.#removeTask(task);
    } else await this.#saveTask(task);
  }

  async #lockTask(task: WithId<Task>) {
    const oldUpdatedAt = task.updatedAt;
    task.updatedAt = new Date();
    task.lockedAt = new Date();
    const { matchedCount, modifiedCount } = await this.#collection.updateOne(
      { _id: task._id, lockedAt: null },
      { $set: task }
    );
    const isLocked = matchedCount === 1 && modifiedCount === 1;
    if (!isLocked) {
      task.updatedAt = oldUpdatedAt;
      task.lockedAt = null;
    }
    return isLocked;
  }

  #saveTask(task: WithId<Task>) {
    return this.#collection.updateOne({ _id: task._id }, { $set: task });
  }

  #removeTask(task: WithId<Task>) {
    return this.#collection.deleteOne({ _id: task._id });
  }
}

// Helpers

const getNextRunAt = (repeatsEvery: number, now = Date.now()) =>
  new Date(now + repeatsEvery);

// Interfaces

export interface TaskManagerOptions {
  concurrency: number;
  lockTimeout: number;
  prolongLockTimeout: number;
  drainTimeout: number;
  removeFinishedTasks: boolean;
  removeFailedTasks: boolean;
}

export interface AddSingleTaskOptions {
  name: string;
  data?: unknown;
  startsAt: Date;
}

export interface AddRepeatingTaskOptions {
  name: string;
  repeatsEvery: number;
}

export interface TaskContext {
  progress: (progress?: string | number) => void;
}

export type SingleTaskContext<TData = unknown> = TaskContext & {
  data: TData | null;
};

export type SingleTaskFn<TData = unknown> = (
  context: SingleTaskContext<TData>
) => void | Promise<void>;

export type RepeatingTaskFn = (
  context: TaskContext
) => boolean | void | Promise<boolean | void>;

export type Task = {
  name: string;
  progress: string | number | null;
  error: string | null;
  startsAt: Date | null;
  lockedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
} & (
  | {
      kind: "single";
      data: unknown | null;
      finishedAt: Date | null;
      failedAt: Date | null;
    }
  | {
      kind: "repeating";
      runs: number;
      repeatsEvery: number;
      lastFailedAt: Date | null;
      lastFinishedAt: Date | null;
    }
);
