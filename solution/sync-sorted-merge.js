"use strict";

const Heap = require("heap");

// Print all entries, across all of the sources, in chronological order.
module.exports = (logSources, printer) => {
  // Note: Chose to use a heap to store log sources in a sorted order.
  // Initial buffering of logs takes O(n*log(n)) time where n is the number of log sources
  // Adding each entry takes log(n) time, so total to print all logs is O(n * m * log(n))) where m is the number of logs in each source
  const heap = new Heap((a, b) => {
    return a.entry.date - b.entry.date;
  });
  // Note: Read first log into memory so we can establish which entry occured earliest
  for (let source of logSources) {
    const logEntry = source.pop();
    if (logEntry) {
      heap.push({
        source: source,
        entry: logEntry
      });
    }
  }
  // Note: Keep printing logs until we reach the end of each stream
  while (!heap.empty()) {
    const earliestItem = heap.pop();
    printer.print(earliestItem.entry);
    const logEntry = earliestItem.source.pop();
    if (logEntry) {
      heap.push({
        source: earliestItem.source,
        entry: logEntry
      });
    }
  }
  printer.done();
  console.log("Sync sort complete.");
};
