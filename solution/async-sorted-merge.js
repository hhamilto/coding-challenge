"use strict";

const Heap = require("heap");

// Note: This variable can be tuned depending on whether we need to be memory efficient or time efficient.
// larger values print logs faster, but use more memory
const MAX_LOG_ENTRY_BUFFER_SIZE = 100000;

// Print all entries, across all of the sources, in chronological order.
module.exports = (logSources, printer) => {
  // Note: Other code in this project uses Promise constructors, so I've continued that style
  //       here. I prefer to write async/await code where possible, but I value keeping code
  //       in a consistent style.
  const heap = new Heap((a, b) => {
    return a.entry.date - b.entry.date;
  });

  let numLogEntriesBuffered = 0;

  // This helper function will buffer logs from the source if we have room in our buffer,
  // and will continue to fill the buffer until full or the log source has been drained.
  const bufferLogs = (sourceContext) => {
    if (numLogEntriesBuffered === MAX_LOG_ENTRY_BUFFER_SIZE) {
      return;
    }
    if (sourceContext.drained) {
      return;
    }
    if (sourceContext.nextEntryPromise) {
      // Note: popAsync() promise is currently in flight, so don't make another call
      return;
    }
    numLogEntriesBuffered++;
    sourceContext.nextEntryPromise = sourceContext.source.popAsync().then(entry => {
      if (entry) {
        sourceContext.bufferedEntries.push(entry);
      } else {
        sourceContext.drained = true;
      }
      sourceContext.nextEntryPromise = null;
      bufferLogs(sourceContext);
    });
  };

  // Note: Fetch first log from each source and build heap
  const promises = [];
  for (let source of logSources) {
    promises.push(source.popAsync().then(logEntry => {
      if (!logEntry) {
        return;
      }
      const sourceContext = {
        source: source,
        entry: logEntry,
        drained: false,
        bufferedEntries: []
      };
      heap.push(sourceContext);
      // Note: kick off log buffering in background if possible
      bufferLogs(sourceContext);
    }));
  }

  // Note: print logs and keep order of next logs using heap until sources are exhausted
  const processNextLogEntry = () => {
    if (heap.empty()) {
      printer.done();
      console.log("Async sort complete.");
      return;
    }
    const earliestSourceContext = heap.pop();
    printer.print(earliestSourceContext.entry);

    // This helper function gets the next log out of the buffer, rebuilds the heap, and kicks off log buffering if necessary
    const getNextEntry = () => {
      numLogEntriesBuffered--;
      earliestSourceContext.entry = earliestSourceContext.bufferedEntries.shift();
      bufferLogs(earliestSourceContext);
      heap.push(earliestSourceContext);
      return processNextLogEntry();
    };

    // CASE 1: We have log entries buffered into memory and can look at the next one and push our context back onto the heap for future printing
    if (earliestSourceContext.bufferedEntries.length) {
      return getNextEntry();
    }
    // CASE 2: We are in the process of fetching the next log into the buffer, so wait for that process to complete
    if (earliestSourceContext.nextEntryPromise) {
      return earliestSourceContext.nextEntryPromise.then(() => {
        // If the log source doesn't have any more logs in it, continue to print rest of logs from other sources
        if (earliestSourceContext.bufferedEntries.length === 0 && earliestSourceContext.drained) {
          return processNextLogEntry();
        }
        return getNextEntry();
      });
    }
    // CASE 3: This log source doesn't have any more logs, so our heap shinks by one and we continue to process remaining logs
    if (earliestSourceContext.drained) {
      return processNextLogEntry();
    }
    // CASE 4: We need to call popAsync() and fetch the next entry. Can happen if our MAX_LOG_ENTRY_BUFFER_SIZE is small and there are no logs buffered
    return earliestSourceContext.source.popAsync().then(entry => {
      if (!entry) {
        // Log source is drained, so continue processing rest of logs
        return processNextLogEntry();
      }
      earliestSourceContext.entry = entry;
      bufferLogs(earliestSourceContext);
      heap.push(earliestSourceContext);
      return processNextLogEntry();
    });
  };
  return Promise.all(promises).then(processNextLogEntry);
};
