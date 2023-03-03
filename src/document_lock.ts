import type { Collection, Filter, UpdateFilter, Document } from "mongodb";

// Public functions

export async function acquireDocumentLock<TDocument extends Document>({
  collection,
  document,
  duration,
  strict = true,
  key = DEFAULT_LOCKED_AT_KEY,
}: AqcuireDocumentLockOptions<TDocument>) {
  const now = new Date();
  const filter = {
    _id: document._id,
    ...(duration !== undefined
      ? {
          [key]: {
            $lte: new Date(now.getTime() - duration),
          },
        }
      : {
          [key]: { $eq: null },
        }),
  } as unknown as Filter<TDocument>;
  const update = {
    $set: { [key]: now },
  } as unknown as UpdateFilter<TDocument>;
  const { matchedCount, modifiedCount } = await collection.updateOne(
    filter,
    update
  );
  const isAcquired = matchedCount === 1 && modifiedCount === 1;
  if (!isAcquired && strict)
    throw new DocumentLockAcquisitionError("Lock not acquired");
  if (isAcquired) (document as Document)[key] = now;
  return isAcquired;
}

export async function releaseDocumentLock<TDocument extends Document>({
  collection,
  document,
  explicit = false,
  strict = false,
  key = DEFAULT_LOCKED_AT_KEY,
}: ReleaseDocumentLockOptions<TDocument>) {
  const filter = {
    _id: document._id,
    ...(explicit ? { [key]: { $ne: null } } : {}),
  } as unknown as Filter<TDocument>;
  const update = {
    $set: { [key]: null },
  } as unknown as UpdateFilter<TDocument>;
  const { matchedCount, modifiedCount } = await collection.updateOne(
    filter,
    update
  );
  const isReleased = matchedCount === 1 && modifiedCount === 1;
  if (!isReleased && strict)
    throw new DocumentLockReleaseError("Lock not released");
  if (isReleased) (document as Document)[key] = null;
  return isReleased;
}

export async function withDocumentLock<
  TDocument extends Document,
  TPromiseResult
>(
  collection: Collection<TDocument>,
  document: TDocument,
  callback: () => Promise<TPromiseResult>
) {
  await acquireDocumentLock({
    collection,
    document,
  });
  return callback().finally(() =>
    releaseDocumentLock({ collection, document })
  );
}

// Errors

export class DocumentLockAcquisitionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LockAcquisitionError";
  }
}

export class DocumentLockReleaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LockReleaseError";
  }
}

// Interfaces

export interface AqcuireDocumentLockOptions<TDocument extends Document> {
  collection: Collection<TDocument>;
  document: TDocument;
  sideEffect?: (lockedAt: Date) => void;
  duration?: number;
  strict?: boolean;
  key?: string;
}

export interface ReleaseDocumentLockOptions<TDocument extends Document> {
  collection: Collection<TDocument>;
  document: TDocument;
  explicit?: boolean;
  strict?: boolean;
  key?: string;
}

// Constants

const DEFAULT_LOCKED_AT_KEY = "lockedAt";
