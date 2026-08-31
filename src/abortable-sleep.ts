export async function abortableSleep(milliseconds: number, signal: AbortSignal | undefined, cancellationError: () => Error): Promise<void> {
	if (signal?.aborted) throw cancellationError();
	await new Promise<void>((resolve, reject) => {
		const removeAbortListener = (): void => signal?.removeEventListener("abort", abort);
		const timeout = setTimeout(() => {
			removeAbortListener();
			resolve();
		}, milliseconds);
		const abort = (): void => {
			clearTimeout(timeout);
			removeAbortListener();
			reject(cancellationError());
		};
		signal?.addEventListener("abort", abort, { once: true });
	});
}
