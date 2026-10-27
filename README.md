# SkyGate: Airport Seat Reservation – Producer–Consumer Lab

SkyGate is a self-contained Operating Systems practical lab for producer–consumer synchronization. Passenger booking requests arrive from web, mobile, kiosk, and travel-agent producers. A bounded FIFO queue feeds booking consumers that update flight AI-204’s shared seat map and available-seat counter.

## Run the lab

Open `index.html` directly in a current browser. No package install, web server, framework, or backend is required. The lab is also static-site friendly for GitHub Pages.

Use the four tabs to explore:

1. **Race condition:** Step or auto-run two consumers through a deterministic unsafe interleaving. Both read the same counter and free seat before either writes; the seat flashes red and the lost counter update is shown.
2. **Mutex solution:** Step the same two requests with a FIFO lock. Watch one agent own the critical section while the other waits, then compare seat bookings and counter.
3. **Empty / full:** Set buffer size to 3–10. Produce until the queue fills, consume FIFO requests, or try consuming an empty queue. Watch semaphore values and FIFO waiter wakeups.
4. **Full simulation:** Adjust producers, consumers, buffer capacity, speeds, and seed. Run, pause, step, or reset. Toggle synchronization to compare safe bookings with a reproducible unsafe race and inspect the live metrics and event trace.

The scheduler is deterministic and tick based. Each runnable agent advances one instruction per tick; it does not start real threads or make real bookings. The counting semaphores track empty/full permits, and the mutex transfers ownership to blocked agents in FIFO order. In-flight semaphore permits are accounted for while a producer or consumer is between `wait` and `signal`.

## Lab objectives

- **Race condition:** The same seat is observed as free by two unsynchronized consumers. A stale write loses one decrement of `seats_available`.
- **Mutual exclusion:** A binary mutex protects the complete seat-map check and update so only one agent can execute the critical section at a time.
- **Bounded buffer:** The request queue has finite capacity N. Producers wait on `empty` when full; consumers wait on `full` when empty.
- **Semaphores:** `empty + full = N` in stable states. During an instruction-level handoff, the lab accounts for reserved and in-flight permits so the total remains N.

## Built-in checks

Click **Run self-checks** at the bottom of the page. Results appear in the header and beside the button; detailed rows are written to the browser console. The checks verify that the unsafe race reproduces with seed 204, mutex synchronization prevents duplicate bookings, the integrated unsafe run reproduces a race, the synchronized integrated run remains race free, semaphore permits remain conserved, and the bounded buffer blocks and wakes producers/consumers correctly.

You can also run the checks by opening `index.html?selfcheck=1`.

## Screenshot suggestions for the practical report

- Race condition after auto-run, showing both agents reading seat 1A, the red double-booked seat, and the expected/actual counter mismatch.
- Mutex mode with one agent holding the lock and the other in the blocked queue.
- Empty/full mode at full capacity and then empty, with semaphore values and blocked states visible.
- Full simulation with synchronization OFF showing the error metric and red seat.
- Full simulation with synchronization ON showing zero double bookings and the event log’s mutex/semaphore trace.
