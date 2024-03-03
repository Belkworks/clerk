import { PromiseQueue } from "@belkworks/promise-queue";

const DataStoreService = game.GetService("DataStoreService");
const Players = game.GetService("Players");

const readQueue = new PromiseQueue();

const writeQueue = new PromiseQueue();

type PendingWrite = {
	value: unknown;
	promise: Promise<string>;
};

export type ClerkOptions = {
	/** DataStore name */
	name: string;

	/** DataStore scope (optional) */
	scope?: string;

	/** How much request bandwidth to use.
	 * Must be greater than 0 and less than or equal to 1.
	 * @default 1
	 */
	bandwidth?: number;
};

export class Clerk {
	private readonly store: DataStore;

	private readonly bandwidth: number;

	constructor(opts: ClerkOptions) {
		this.store = DataStoreService.GetDataStore(opts.name, opts.scope);
		this.bandwidth = opts.bandwidth ?? 1;
		assert(this.bandwidth > 0, "clerk: bandwidth must be greater than 0!");
		assert(this.bandwidth <= 1, "clerk: bandwidth must be less than or equal to 1!");
	}

	private getMinimumRequestTime() {
		const numPlayers = Players.GetPlayers().size();
		return 60 / ((60 + numPlayers * 10) * this.bandwidth);
	}

	private async doThrottled<T>(factory: () => Promise<T>) {
		const delay = Promise.delay(this.getMinimumRequestTime());

		try {
			return await factory();
		} finally {
			await delay;
		}
	}

	private readonly reads = new Map<string, Promise<unknown>>();

	read(key: string, unshift?: boolean) {
		const read = this.reads.get(key);
		if (read) return read;

		const factory = () => this.doThrottled(async () => this.store.GetAsync(key));
		const promise = readQueue.push(factory, unshift);

		this.reads.set(key, promise);
		promise.finally(() => this.reads.delete(key));

		return promise;
	}

	private readonly writes = new Map<string, PendingWrite>();

	async write(key: string, value: unknown, unshift?: boolean) {
		const pending = this.writes.get(key);
		if (pending) {
			pending.value = value;
			return pending.promise;
		}

		const entry: PendingWrite = {
			value,
			promise: Promise.resolve(""),
		};

		const factory = () => {
			this.writes.delete(key);
			return this.doThrottled(async () => this.store.SetAsync(key, entry.value));
		};

		const promise = writeQueue.push(factory, unshift);

		entry.promise = promise;
		this.writes.set(key, entry);

		promise.finally(() => this.writes.delete(key));

		return promise;
	}
}
