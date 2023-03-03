import type { Collection, ObjectId } from "mongodb";

// Public classes

export class CollectionSet<TValue> {
  #collection: Collection;
  #key: string;

  constructor(collection: Collection, key: string) {
    this.#collection = collection;
    this.#key = key;
  }

  createIndexes() {
    return createIndexesForSet(this.#collection);
  }

  values() {
    return getSetMembers<TValue>(this.#collection, this.#key);
  }

  add(values: TValue[]) {
    return addToSet(this.#collection, this.#key, values);
  }

  remove(values: TValue[]) {
    return removeFromSet(this.#collection, this.#key, values);
  }

  has(value: TValue) {
    return isMemberOfSet(this.#collection, this.#key, value);
  }
}

// Public functions

export async function createIndexesForSet(collection: Collection) {
  await collection.createIndex({ key: 1 });
  await collection.createIndex({ key: 1, value: 1 }, { unique: true });
}

export async function addToSet<TValue>(
  collection: Collection,
  key: string,
  values: TValue[],
  {
    createdAtKey = DEFAULT_CREATED_AT_KEY,
    updatedAtKey = DEFAULT_UPDATED_AT_KEY,
  }: AddToSetOptions = {}
): Promise<Record<number, ObjectId>> {
  const now = new Date();
  const documents = values.map((value) => ({
    key,
    value,
    [createdAtKey]: now,
    [updatedAtKey]: now,
  }));
  const { insertedIds } = await collection.insertMany(documents, {
    ordered: false,
  });
  return insertedIds;
}

export async function removeFromSet<TValue>(
  collection: Collection,
  key: string,
  values: TValue[]
): Promise<number> {
  const { deletedCount } = await collection.deleteMany({
    key,
    value: { $in: values },
  });
  return deletedCount;
}

export async function getSetMembers<TValue>(
  collection: Collection,
  key: string
) {
  const documents = await collection.find({ key }).toArray();
  const values = documents.map((document) => document.value);
  return values as TValue[];
}

export async function isMemberOfSet<TValue>(
  collection: Collection,
  key: string,
  value: TValue
) {
  const document = await collection.findOne({ key, value });
  return document !== null;
}

// Interfaces

export interface AddToSetOptions {
  createdAtKey?: string;
  updatedAtKey?: string;
}

// Constants

const DEFAULT_CREATED_AT_KEY = "createdAt";

const DEFAULT_UPDATED_AT_KEY = "updatedAt";
