/* SkyGate OS lab. All agents advance deterministically; this is a teaching simulation, not real threads. */
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const SOURCE_TYPES = ['Web', 'Mobile app', 'Kiosk', 'Travel agent'];
  const PASSENGERS = ['Alex Morgan', 'Jamie Patel', 'Sam Rivera', 'Taylor Chen', 'Jordan Singh', 'Casey Brown', 'Riley Khan'];
  const SEAT_NAMES = Array.from({ length: 6 }, (_, row) => ['A', 'B', 'C', 'D', 'E', 'F'].map(letter => `${row + 1}${letter}`)).flat();
  const RACE_LINES = ['read seats_available', 'read first_free_seat', 'write seat_map[seat] = passenger', 'write seats_available = snapshot - 1'];
  const PRODUCER_CODE = ['wait(empty)', 'wait(mutex)', 'insert(request, buffer[tail])', 'tail = (tail + 1) mod N', 'signal(mutex)', 'signal(full)'];
  const CONSUMER_CODE = ['wait(full)', 'wait(mutex)', 'request = remove(buffer[head])', 'head = (head + 1) mod N', 'signal(mutex)', 'signal(empty)', 'wait(seat_mutex)', 'read/check/book seat', 'seats_available = seats_available - 1', 'signal(seat_mutex)'];
  const PHASE_LABEL = { 'read-counter':'Read shared counter', 'read-seat':'Read first free seat', 'write':'Write seat and counter', 'acquire':'Acquire mutex', 'release':'Release mutex', 'wait-empty':'wait(empty)', 'wait-mutex':'wait(mutex)', insert:'Insert request', 'signal-mutex':'signal(mutex)', 'signal-full':'signal(full)', 'wait-full':'wait(full)', remove:'Remove oldest request', 'signal-empty':'signal(empty)', 'lock-seat':'Acquire seat mutex', 'release-seat':'Release seat mutex' };
  let activeMode = 'race';
  let raceState, mutexState, bufferState, fullState;
  let raceTimer = null, mutexTimer = null, fullTimer = null;
  let selfCheckStatus = null;

  const timestamp = tick => `T+${String(tick).padStart(3, '0')}`;
  function addLog(element, tick, type, message, limit = 60) {
    const row = document.createElement('div'); row.className = `log-entry ${type}`;
    const time = document.createElement('time'); time.textContent = timestamp(tick);
    const text = document.createElement('span'); text.textContent = message;
    row.append(time, text); element.prepend(row);
    while (element.children.length > limit) element.lastElementChild.remove();
  }
  function clearLog(element) { element.replaceChildren(); }
  function seeded(seed) { let value = (Number(seed) >>> 0) || 1; return () => { value = (Math.imul(value, 1664525) + 1013904223) >>> 0; return value / 4294967296; }; }
  function makeRequest(id, producer, random) {
    return { id, producer, passenger: PASSENGERS[Math.floor(random() * PASSENGERS.length)], source: SOURCE_TYPES[Math.floor(random() * SOURCE_TYPES.length)], flight: 'AI-204' };
  }
  function seatMapHtml(bookings = new Map()) {
    return SEAT_NAMES.map((seat, index) => {
      const occupants = bookings.get(seat) || [];
      const letter = seat.slice(-1), column = 'ABC'.includes(letter) ? letter.charCodeAt(0) - 64 : letter.charCodeAt(0) - 63;
      const booked = occupants.length > 0, conflict = occupants.length > 1;
      return `<div class="seat${booked ? ' booked' : ''}${conflict ? ' double' : ''}" style="grid-column:${column};grid-row:${Math.floor(index / 6) + 1}" title="${seat}${booked ? ` · ${occupants.map(x => x.passenger || x).join(', ')}` : ' · Free'}" data-initial="${booked ? (conflict ? '×' : occupants[0].passenger?.[0] || '✓') : ''}">${seat}</div>`;
    }).join('');
  }
  function renderSeatMap(id, bookings) { $(id).innerHTML = seatMapHtml(bookings); }
  function setRaceCode(id, phase, sync = false) {
    const labels = sync ? ['wait(mutex)', ...RACE_LINES, 'signal(mutex)'] : RACE_LINES;
    const current = sync ? ({ acquire:0, 'read-counter':1, 'read-seat':2, write:3, release:4 }[phase]) : ({ 'read-counter':0, 'read-seat':1, write:3 }[phase]);
    $(id).innerHTML = labels.map((line, i) => `<div class="code-line${i === current ? ' current' : ''}${line.startsWith('write') ? ' error' : ''}"><span>${String(i + 1).padStart(2, '0')}</span>  ${line.replace(/(wait|signal|read|write)/, '<em>$1</em>')}</div>`).join('');
  }
  function stopTimer(which) { if (which === 'race' && raceTimer) clearInterval(raceTimer); if (which === 'mutex' && mutexTimer) clearInterval(mutexTimer); if (which === 'full' && fullTimer) clearInterval(fullTimer); if (which === 'race') raceTimer = null; if (which === 'mutex') mutexTimer = null; if (which === 'full') fullTimer = null; }

  // FIFO counting semaphore: a signalled waiter receives the permit directly.
  class CountingSemaphore {
    constructor(count, name, log) { this.count = count; this.name = name; this.queue = []; this.reserved = new Set(); this.log = log; }
    wait(agent) {
      if (this.reserved.delete(agent.id)) { agent.status = 'ready'; this.log('lock', `${agent.id} resumed with a ${this.name} permit.`); return true; }
      if (this.count > 0) { this.count--; this.log('lock', `${agent.id}: wait(${this.name}) succeeds; ${this.name}=${this.count}.`); return true; }
      if (!this.queue.includes(agent.id)) this.queue.push(agent.id);
      agent.status = 'blocked'; agent.blockedOn = this.name;
      this.log('blocked', `${agent.id} blocks on wait(${this.name}); FIFO waiters: ${this.queue.join(' → ')}.`); return false;
    }
    signal(agent, agents) {
      const nextId = this.queue.shift();
      if (nextId) { this.reserved.add(nextId); const next = agents.find(x => x.id === nextId); if (next) { next.status = 'ready'; next.blockedOn = null; } this.log('lock', `${agent.id}: signal(${this.name}) wakes ${nextId} (FIFO).`); }
      else { this.count++; this.log('lock', `${agent.id}: signal(${this.name}); ${this.name}=${this.count}.`); }
    }
  }
  // Binary ownership lock with FIFO handoff.
  class Mutex {
    constructor(name, log) { this.name = name; this.owner = null; this.queue = []; this.log = log; }
    wait(agent) {
      if (this.owner === agent.id) return true;
      if (!this.owner) { this.owner = agent.id; agent.status = 'ready'; this.log('lock', `${agent.id} acquired ${this.name}.`); return true; }
      if (!this.queue.includes(agent.id)) this.queue.push(agent.id);
      agent.status = 'blocked'; agent.blockedOn = this.name;
      this.log('blocked', `${agent.id} waits for ${this.name}; held by ${this.owner}.`); return false;
    }
    signal(agent, agents) {
      if (this.owner !== agent.id) return;
      const nextId = this.queue.shift(); this.owner = nextId || null;
      if (nextId) { const next = agents.find(x => x.id === nextId); if (next) { next.status = 'ready'; next.blockedOn = null; } this.log('lock', `${agent.id} released ${this.name}; ownership handed to ${nextId}.`); }
      else this.log('lock', `${agent.id} released ${this.name}; lock is UNLOCKED.`);
    }
  }

  function newRace(sync) {
    stopTimer(sync ? 'mutex' : 'race');
    $(`${sync?'mutex':'race'}Auto`).textContent='Auto-run';
    const bookings = new Map();
    const agents = ['Agent A', 'Agent B'].map((id, i) => ({ id, passenger: ['Avery Lee', 'Morgan Shah'][i], phase: sync ? 'acquire' : 'read-counter', status: 'ready', counter: null, seat: null }));
    const state = { tick: 0, cursor: 0, counter: 36, expected: 36, bookings, double: 0, agents, sync, mutex: null, logId: sync ? 'mutexLog' : 'raceLog' };
    const logger = (type, message) => addLog($(state.logId), state.tick, type, message);
    state.mutex = new Mutex('seat_mutex', logger);
    if (sync) { mutexState = state; clearLog($('mutexLog')); }
    else { raceState = state; clearLog($('raceLog')); }
    return state;
  }
  function renderRace(state) {
    const sync = state.sync, prefix = sync ? 'mutex' : 'race';
    $(`${prefix}Tick`).textContent = state.tick;
    $(`${prefix}Counter`).textContent = state.counter;
    if (!sync) $('raceActual').textContent = state.counter;
    const agentRoot = $(`${prefix}Agents`);
    agentRoot.innerHTML = state.agents.map(a => `<div class="race-agent ${a.status === 'blocked' ? 'blocked' : a.status === 'done' ? 'done' : a.id === state.lastAgent ? 'active' : ''}"><div class="agent-title"><span>${a.id}</span><span class="agent-state">${a.status === 'blocked' ? 'BLOCKED' : a.status === 'done' ? 'DONE' : a.id === state.lastAgent ? 'EXECUTING' : 'READY'}</span></div><div class="agent-instruction">${a.status === 'done' ? `Booked ${a.seat || 'seat'} for ${a.passenger}` : PHASE_LABEL[a.phase] || a.phase}</div></div>`).join('');
    renderSeatMap(sync ? 'mutexSeatMap' : 'raceSeatMap', state.bookings);
    const done = state.agents.every(a => a.status === 'done');
    if (sync) {
      $('mutexState').classList.toggle('locked', Boolean(state.mutex.owner)); $('mutexState').innerHTML = state.mutex.owner ? '<i>🔒</i> LOCKED' : '<i>🔓</i> UNLOCKED';
      $('lockOwner').textContent = state.mutex.owner ? `LOCKED · held by ${state.mutex.owner}` : 'No owner · lock is available';
      $('mutexQueue').textContent = state.mutex.queue.join(' → ') || 'No agents waiting';
      $('mutexBadge').textContent = done ? 'MUTUAL EXCLUSION VERIFIED' : 'READY TO RUN';
      $('mutexResult').textContent = done ? `${state.double === 0 && state.counter === 34 ? 'PASS — ' : 'CHECK — '}No double booking: ${state.double}. Counter ${state.counter} (expected 34).` : 'The lock owner executes the whole transaction before the next agent enters.';
    } else {
      $('raceBadge').textContent = state.double ? 'RACE CONDITION DETECTED' : done ? 'RACE NOT REPRODUCED' : 'RACE NOT STARTED';
      $('raceBadge').className = `badge ${state.double ? 'badge-red' : 'badge-blue'}`;
      $('raceActual').classList.toggle('seat-counter-error', state.counter !== state.expected);
    }
    const active = state.agents.find(a => a.id === state.lastAgent);
    if (active) setRaceCode(sync ? 'mutexCode' : 'raceCode', active.phase, sync);
    $('raceStep').disabled = state.agents.every(a => a.status === 'done');
    $('mutexStep').disabled = state.agents.every(a => a.status === 'done');
  }
  function stepRace(state) {
    if (state.agents.every(a => a.status === 'done')) return;
    state.tick++;
    let agent = null;
    for (let tries = 0; tries < state.agents.length; tries++) {
      const candidate = state.agents[state.cursor % state.agents.length]; state.cursor++;
      if (candidate.status === 'ready') { agent = candidate; break; }
    }
    if (!agent) { addLog($(state.logId), state.tick, 'blocked', 'No runnable agent; the next lock owner must be signalled.'); renderRace(state); return; }
    state.lastAgent = agent.id;
    const log = (type, message) => addLog($(state.logId), state.tick, type, message);
    if (state.sync && agent.phase === 'acquire') {
      if (!state.mutex.wait(agent)) { renderRace(state); return; }
      agent.phase = 'read-counter'; log('consumer', `${agent.id} enters the protected critical section.`);
    } else if (agent.phase === 'read-counter') {
      agent.counter = state.counter; agent.phase = 'read-seat'; log('consumer', `${agent.id} READ seats_available=${agent.counter}.`);
    } else if (agent.phase === 'read-seat') {
      agent.seat = SEAT_NAMES.find(seat => !state.bookings.has(seat)) || 'FULL'; agent.phase = 'write';
      log('consumer', `${agent.id} READ seat ${agent.seat} as free${state.sync ? '' : ' (no lock held)'}.`);
      if (!state.sync) log('blocked', 'CONTEXT SWITCH — both agents may have cached the same free seat.');
    } else if (agent.phase === 'write') {
      if (agent.seat === 'FULL') { log('blocked', `${agent.id} found no remaining seat.`); agent.status = 'done'; }
      else {
        const occupants = state.bookings.get(agent.seat) || []; occupants.push({ passenger: agent.passenger }); state.bookings.set(agent.seat, occupants);
        state.expected--; state.counter = agent.counter - 1;
        if (occupants.length > 1) { state.double++; log('error', `DOUBLE BOOKING: ${agent.seat} was already written by another agent.`); }
        log(state.double ? 'error' : 'consumer', `${agent.id} WRITE ${agent.passenger} → ${agent.seat}; counter becomes ${state.counter}.`);
        if (!state.sync) agent.status = 'done'; else agent.phase = 'release';
      }
    } else if (agent.phase === 'release') { state.mutex.signal(agent, state.agents); agent.status = 'done'; log('lock', `${agent.id} exits the critical section.`); }
    renderRace(state);
  }
  function autoRace(sync) {
    const state = sync ? mutexState : raceState, which = sync ? 'mutex' : 'race';
    if (!state || state.agents.every(a => a.status === 'done')) return;
    if (sync ? mutexTimer : raceTimer) { stopTimer(which); $(`${which}Auto`).textContent = 'Auto-run'; return; }
    $(`${which}Auto`).textContent = 'Pause';
    const timer = setInterval(() => { stepRace(state); if (state.agents.every(a => a.status === 'done')) { stopTimer(which); $(`${which}Auto`).textContent = 'Auto-run'; } }, 650);
    if (sync) mutexTimer = timer; else raceTimer = timer;
  }

  // Standalone bounded-buffer lab. Each click runs the actual semaphore protocol atomically and logs each wait/signal.
  function makeBufferMode(capacity) {
    bufferState = { capacity, queue: [], head: 0, tail: 0, serial: 0, producerWait: [], consumerWait: [], empty: capacity, full: 0, lock: new Mutex('buffer mutex', (type, msg) => addLog($('bufferLog'), 0, type, msg)), logTick: 0 };
    clearLog($('bufferLog')); renderBufferMode();
  }
  function renderBufferMode() {
    const s = bufferState, n = s.capacity;
    $('emptyValue').textContent = s.empty; $('fullValue').textContent = s.full;
    $('emptyTrack').style.width = `${s.empty / n * 100}%`; $('fullTrack').style.width = `${s.full / n * 100}%`;
    $('semaphoreInvariant').textContent = `empty + full = ${s.empty + s.full} (N = ${n})`;
    $('bufferMutex').textContent = s.lock.owner ? `🔒 Held by ${s.lock.owner}` : '🔓 Unlocked';
    $('bufferProducerWaiters').textContent = s.producerWait.map(x => x.id).join(' → ') || 'None';
    $('bufferConsumerWaiters').textContent = s.consumerWait.map(x => x.id).join(' → ') || 'None';
    $('producerExplanation').textContent = s.empty === 0 ? 'Buffer is full. Producers sleep on wait(empty) until a slot is signalled.' : 'The producer can insert while empty > 0.';
    $('consumerExplanation').textContent = s.full === 0 ? 'Buffer is empty. Consumer sleeps on wait(full) until work is signalled.' : 'A request is ready; consumer removes the oldest item (FIFO).';
    $('bufferProducerState').textContent = s.empty ? 'READY' : 'BLOCKED'; $('bufferProducerState').className = `state-pill ${s.empty ? 'state-ready' : 'state-blocked'}`;
    $('bufferConsumerState').textContent = s.full ? 'READY' : 'SLEEPING'; $('bufferConsumerState').className = `state-pill ${s.full ? 'state-ready' : 'state-sleep'}`;
    $('headPointer').textContent = `HEAD → ${s.queue.length ? (s.head % n) + 1 : '—'}`; $('tailPointer').textContent = `TAIL → ${(s.tail % n) + 1}`;
    $('bufferSlots').innerHTML = Array.from({ length:n }, (_,i) => { const item=s.queue[i]; return `<div class="buffer-slot ${item?'filled':''}">${i===s.head%n&&item?'<span class="head-tag">HEAD</span>':''}${i===(s.tail-1+n)%n&&item?'<span class="tail-tag">TAIL</span>':''}${item?`<span class="slot-id">${item.id}</span><span class="slot-name">${escapeHtml(item.passenger)}</span>`:`<span>SLOT ${i+1}</span><span>EMPTY</span>`}</div>`; }).join('');
    $('bufferBadge').textContent = s.empty === 0 ? 'PRODUCERS BLOCKED · FULL' : s.full === 0 ? 'CONSUMERS SLEEP · EMPTY' : 'SYNCHRONIZED';
  }
  function bufferProduce() {
    const s = bufferState, log = (type,msg) => addLog($('bufferLog'), ++s.logTick, type, msg);
    const p = { id:`P${++s.serial}`, passenger:$('passengerName').value.trim()||'Passenger', source:$('sourceType').value, flight:'AI-204' };
    log('producer', `${p.source} producer created ${p.id} for ${p.passenger}.`);
    if (!s.empty) { s.producerWait.push(p); log('blocked', `${p.id}: wait(empty) blocks; empty=0, buffer full.`); renderBufferMode(); return; }
    s.empty--; log('lock', `${p.id}: wait(empty) succeeds; empty=${s.empty}.`);
    if (s.lock.owner) { s.producerWait.push(p); log('blocked', `${p.id} waits on buffer mutex.`); renderBufferMode(); return; }
    s.lock.wait({ id:p.id }); s.queue.push(p); s.tail=(s.tail+1)%s.capacity; s.full++; log('producer', `${p.id} inserted at tail; full=${s.full}.`);
    s.lock.signal({ id:p.id });
    if (s.consumerWait.length) { const c=s.consumerWait.shift(); log('lock', `signal(full) wakes consumer ${c} in FIFO order.`); }
    else log('lock', `signal(full); full=${s.full}.`);
    renderBufferMode();
  }
  function bufferConsume() {
    const s = bufferState, log = (type,msg) => addLog($('bufferLog'), ++s.logTick, type, msg);
    const c = `C${s.logTick+1}`;
    if (!s.full) { s.consumerWait.push(c); log('blocked', `${c}: wait(full) blocks; full=0, buffer empty. Consumer sleeps.`); renderBufferMode(); return; }
    s.full--; log('lock', `${c}: wait(full) succeeds; full=${s.full}.`);
    const item=s.queue.shift(); s.head=(s.head+1)%s.capacity;
    if (s.lock.owner) { log('blocked', `${c} waits on buffer mutex.`); renderBufferMode(); return; }
    s.lock.wait({ id:c }); log('consumer', `${c} removed oldest ${item.id} (${item.passenger}); FIFO order preserved.`); s.lock.signal({ id:c });
    if (s.producerWait.length) { const p=s.producerWait.shift(); s.queue.push(p); s.tail=(s.tail+1)%s.capacity; s.full++; log('lock', `signal(empty) wakes producer ${p.id}; its reserved slot is inserted at tail.`); log('lock', `signal(full) wakes a consumer if waiting.`); }
    else { s.empty++; log('lock', `signal(empty); empty=${s.empty}.`); }
    if (s.consumerWait.length && s.full>0) { const w=s.consumerWait.shift(); log('lock', `signal(full) wakes ${w} in FIFO order.`); }
    renderBufferMode();
  }

  class FullSimulation {
    constructor(config) { this.reset(config, !config.quiet); }
    reset(config = {}, draw = true) {
      this.stop(); this.tick=0; this.capacity=Number(config.capacity||5); this.producerCount=Number(config.producers||2); this.consumerCount=Number(config.consumers||2);
      this.producerSpeed=Number(config.producerSpeed||3); this.consumerSpeed=Number(config.consumerSpeed||3); this.sync=config.sync!==false; this.seed=Number(config.seed||204);
      this.random=seeded(this.seed); this.buffer=[]; this.head=0; this.tail=0; this.counter=36; this.bookings=new Map(); this.double=0;
      this.produced=0; this.consumed=0; this.blockedProducer=0; this.blockedConsumer=0; this.log=[]; this.agents=[]; this.serial=0; this.cursor=0; this.running=false; this.started=Date.now();
      this.reservedEmpty=0;this.reservedFull=0;this.pendingFull=0;this.pendingEmpty=0;this.invariantFailures=0;
      this.empty=new CountingSemaphore(this.capacity,'empty',(t,m)=>this.event(t,m)); this.full=new CountingSemaphore(0,'full',(t,m)=>this.event(t,m));
      this.bufferMutex=new Mutex('buffer_mutex',(t,m)=>this.event(t,m)); this.seatMutex=new Mutex('seat_mutex',(t,m)=>this.event(t,m));
      this.consumers=Array.from({length:this.consumerCount},(_,i)=>({id:`C${i+1}`,type:'consumer',phase:'wait-full',status:'ready',blockedOn:null}));
      this.nextProduce=Array(this.producerCount).fill(1); this.event('success',`Simulation reset with seed ${this.seed}; ${this.sync?'synchronization ON':'synchronization OFF'}.`); if(draw)this.render();
    }
    stop() { if (fullTimer) clearInterval(fullTimer); fullTimer=null; this.running=false; }
    event(type,message) { const entry={tick:this.tick,type,message}; this.log.unshift(entry); if(this.log.length>100)this.log.pop(); }
    makeProducer(index) {
      const id=`P${++this.serial}`, req=makeRequest(`R${this.produced+1}`,id,this.random); this.produced++;
      const p={...req,requestId:req.id,id,type:'producer',phase:'wait-empty',status:'ready',blockedOn:null,slot:null}; this.agents.push(p);
      this.event('producer',`${p.source} producer ${id} created ${req.id} for ${req.passenger}.`);
    }
    wakeUnsafeConsumers() { for(const c of this.consumers) if(c.status==='blocked'&&c.blockedOn==='full'){c.status='ready';c.blockedOn=null;c._everCounted=false;} }
    tryWait(agent, sem) { const handed=sem.reserved.has(agent.id);if(sem.wait(agent)){if(this.sync&&!handed){if(sem===this.empty)this.reservedEmpty++;if(sem===this.full)this.reservedFull++;}return true;} if(agent.blockedOn===sem.name&&agent.status==='blocked'&&!agent._counted){agent._counted=true;if(agent.type==='producer')this.blockedProducer++;else this.blockedConsumer++;}return false; }
    doStep(draw=true) {
      this.tick++;
      const pInterval=Math.max(1,6-this.producerSpeed), cInterval=Math.max(1,6-this.consumerSpeed);
      for(let i=0;i<this.producerCount;i++) if(this.tick>=this.nextProduce[i]) {this.makeProducer(i);this.nextProduce[i]=this.tick+pInterval;}
      const all=[...this.agents.filter(a=>a.status==='ready'),...this.consumers.filter(a=>a.status==='ready')];
      if(!all.length){this.event('blocked','All agents are blocked; waiting for a semaphore or lock signal.');if(draw)this.render();return;}
      const start=this.cursor%all.length, order=[...all.slice(start),...all.slice(0,start)];this.cursor=(this.cursor+1)%all.length;
      for(const agent of order){
        if(agent.status!=='ready'||(agent.type==='consumer'&&this.tick%cInterval!==0))continue;
        agent._counted=false;
        if(agent.type==='producer')this.stepProducer(agent);else this.stepConsumer(agent);
        if(this.sync&&this.empty.count+this.full.count+this.reservedEmpty+this.reservedFull+this.pendingFull+this.pendingEmpty!==this.capacity)this.invariantFailures=(this.invariantFailures||0)+1;
      }
      if(draw)this.render();
    }
    stepProducer(a) {
      const log=(type,msg)=>this.event(type,msg);
      if(a.phase==='wait-empty'){
        if(this.sync&&!this.tryWait(a,this.empty))return;
        a.phase=this.sync?'wait-mutex':'insert';if(!this.sync)log('producer',`${a.id} proceeds without wait(empty) protection.`);return;
      }
      if(a.phase==='wait-mutex'){if(this.sync&&!this.bufferMutex.wait(a))return;a.phase='insert';return;}
      if(a.phase==='insert'){
        if(this.buffer.length>=this.capacity&&!this.sync)log('error',`BUFFER OVERFLOW: ${a.requestId||a.id} writes past N=${this.capacity}.`);
        a.slot=this.tail%this.capacity;this.buffer.push(a);this.tail++;if(this.sync){this.reservedEmpty--;this.pendingFull++;}log('producer',`${a.id} inserts ${a.requestId} (${a.passenger}) at tail slot ${a.slot+1}.`);
        if(!this.sync)this.wakeUnsafeConsumers();a.phase=this.sync?'signal-mutex':'done';return;
      }
      if(a.phase==='signal-mutex'){this.bufferMutex.signal(a,[...this.agents,...this.consumers]);a.phase='signal-full';return;}
      if(a.phase==='signal-full'){if(this.sync){this.pendingFull--;if(this.full.queue.length)this.reservedFull++;}this.full.signal(a,this.consumers);a.phase='done';this.wakeUnsafeConsumers();return;}
    }
    stepConsumer(a) {
      const log=(type,msg)=>this.event(type,msg);
      if(a.phase==='wait-full'){
        if(this.sync&&!this.tryWait(a,this.full))return;
        if(!this.sync&&!this.buffer.length){a.status='blocked';a.blockedOn='full';if(!a._everCounted){this.blockedConsumer++;a._everCounted=true;}log('blocked',`${a.id} sleeps: buffer empty (wait(full)).`);return;}
        a.phase=this.sync?'wait-mutex':'remove';return;
      }
      if(a.phase==='wait-mutex'){if(this.sync&&!this.bufferMutex.wait(a))return;a.phase='remove';return;}
      if(a.phase==='remove'){
        if(!this.buffer.length){a.phase='wait-full';return;}
        a.request=this.buffer.shift();this.head++;if(this.sync){this.reservedFull--;this.pendingEmpty++;}log('consumer',`${a.id} removes ${a.request.requestId} for ${a.request.passenger} from FIFO head.`);
        a.phase=this.sync?'signal-mutex':'lock-seat';return;
      }
      if(a.phase==='signal-mutex'){this.bufferMutex.signal(a,[...this.agents,...this.consumers]);a.phase='signal-empty';return;}
      if(a.phase==='signal-empty'){if(this.sync){this.pendingEmpty--;if(this.empty.queue.length)this.reservedEmpty++;}this.empty.signal(a,this.agents);a.phase='lock-seat';return;}
      if(a.phase==='lock-seat'){if(this.sync&&!this.seatMutex.wait(a))return;a.phase='read-counter';return;}
      if(a.phase==='read-counter'){a.counterSnapshot=this.counter;a.phase='read-seat';log('consumer',`${a.id} READ seats_available=${a.counterSnapshot}.`);return;}
      if(a.phase==='read-seat'){a.seat=SEAT_NAMES.find(seat=>!this.bookings.has(seat))||'FULL';a.phase='write-seat';log('consumer',`${a.id} READ seat ${a.seat} as free.`);return;}
      if(a.phase==='write-seat'){
        if(a.seat==='FULL'){log('blocked',`${a.id} found no unbooked seat for ${a.request.id}.`);}
        else {const occupants=this.bookings.get(a.seat)||[];occupants.push(a.request);this.bookings.set(a.seat,occupants);this.counter=a.counterSnapshot-1;this.consumed++;
          if(occupants.length>1){this.double++;log('error',`DOUBLE BOOKING: ${a.seat} assigned to ${occupants.map(x=>x.requestId).join(' and ')}; stale counter write → ${this.counter}.`);}
          else log('success',`${a.request.requestId} booked ${a.seat} for ${a.request.passenger}; seats_available=${this.counter}.`);
        }
        a.phase=this.sync?'release-seat':'wait-full';if(!this.sync){a.status='ready';a.request=null;}return;
      }
      if(a.phase==='release-seat'){this.seatMutex.signal(a,this.consumers);a.phase='wait-full';a.request=null;a.status='ready';return;}
    }
    setSync(value) {
      this.sync=value;this.bufferMutex.owner=null;this.bufferMutex.queue=[];this.seatMutex.owner=null;this.seatMutex.queue=[];
      for(const a of [...this.agents,...this.consumers])if(a.status==='blocked'){a.status='ready';a.blockedOn=null;a._counted=false;}
      if(value){
        const interrupted=this.consumers.filter(c=>c.request&&c.phase!=='release-seat').map(c=>c.request);
        for(let i=interrupted.length-1;i>=0;i--)this.buffer.unshift(interrupted[i]);
        const queued=new Set(this.buffer);
        for(const p of this.agents){if(queued.has(p)){p.phase='done';p.status='done';}else if(p.phase!=='done'){p.phase='wait-empty';p.status='ready';p.slot=null;}}
        for(const c of this.consumers){c.phase='wait-full';c.request=null;c.status='ready';c.blockedOn=null;}
        if(this.buffer.length>this.capacity){const overflow=this.buffer.splice(this.capacity);for(const p of overflow){p.phase='wait-empty';p.status='ready';p.slot=null;}}
        this.head=0;this.tail=this.buffer.length;
      }
      this.reservedEmpty=0;this.reservedFull=0;this.pendingFull=0;this.pendingEmpty=0;this.invariantFailures=0;
      this.empty=new CountingSemaphore(Math.max(0,this.capacity-this.buffer.length),'empty',(t,m)=>this.event(t,m));this.full=new CountingSemaphore(this.buffer.length,'full',(t,m)=>this.event(t,m));
      this.event(value?'success':'error',`Synchronization switched ${value?'ON':'OFF'} at tick ${this.tick}; buffer state retained, wait queues released.`);this.render();
    }
    configureFromControls() {
      const producers=Number($('producerCount').value), consumers=Number($('consumerCount').value);
      if(producers>this.producerCount)for(let i=this.producerCount;i<producers;i++)this.nextProduce.push(this.tick+1);
      else if(producers<this.producerCount)this.nextProduce.length=producers;
      if(consumers<this.consumers.length){const removed=this.consumers.splice(consumers);for(const c of removed){this.full.queue=this.full.queue.filter(id=>id!==c.id);this.full.reserved.delete(c.id);this.bufferMutex.queue=this.bufferMutex.queue.filter(id=>id!==c.id);this.seatMutex.queue=this.seatMutex.queue.filter(id=>id!==c.id);}}
      else if(consumers>this.consumers.length)for(let i=this.consumers.length;i<consumers;i++)this.consumers.push({id:`C${i+1}`,type:'consumer',phase:'wait-full',status:'ready',blockedOn:null});
      this.producerCount=producers;this.consumerCount=consumers;this.producerSpeed=Number($('producerSpeed').value);this.consumerSpeed=Number($('consumerSpeed').value);
    }
    render() {
      $('simTick').textContent=this.tick;$('metricProduced').textContent=this.produced;$('metricConsumed').textContent=this.consumed;
      $('metricBlockedP').textContent=this.blockedProducer;$('metricBlockedC').textContent=this.blockedConsumer;$('metricRaces').textContent=this.double;
      $('metricBuffer').textContent=`${Math.round(this.buffer.length/Math.max(1,this.capacity)*100)}%`;$('metricSeats').textContent=this.counter;$('simSeatsAvailable').textContent=this.counter;
      $('metricThroughput').textContent=`${Math.round(this.consumed/Math.max(1,(Date.now()-this.started)/60000))} / min`;
      $('simSemaphores').textContent=`empty ${this.empty.count} · full ${this.full.count} · mutex ${this.bufferMutex.owner?'0':'1'}`;
      $('simPointers').textContent=`HEAD ${this.buffer.length?((this.head%this.capacity)+1):'—'} · TAIL ${(this.tail%this.capacity)+1}`;
      $('simAgentStates').textContent=`${this.consumers.map(c=>`${c.id}:${c.status==='blocked'?'sleep':c.request?.requestId||PHASE_LABEL[c.phase]||c.phase}`).join('  ')}${this.agents.filter(a=>a.status==='blocked').length?` · ${this.agents.filter(a=>a.status==='blocked').length} producer(s) blocked`:''}`;
      $('simLocks').textContent=`buffer_mutex ${this.bufferMutex.owner?`held by ${this.bufferMutex.owner}`:'unlocked'} · seat_mutex ${this.seatMutex.owner?`held by ${this.seatMutex.owner}`:'unlocked'} · waiting ${[...this.bufferMutex.queue,...this.seatMutex.queue].join(' → ')||'none'}`;
      $('simBufferSlots').innerHTML=Array.from({length:this.capacity},(_,i)=>{const item=this.buffer[i];return `<div class="buffer-slot ${item?'filled':''}">${i===0&&item?'<span class="head-tag">HEAD</span>':''}${i===this.buffer.length-1&&item?'<span class="tail-tag">TAIL</span>':''}${item?`<span class="slot-id">${item.requestId}</span><span class="slot-name">${item.passenger}</span>`:`<span>SLOT ${i+1}</span><span>EMPTY</span>`}</div>`}).join('');
      renderSeatMap('simSeatMap',this.bookings);
      $('simLog').innerHTML=this.log.slice(0,40).map(e=>`<div class="log-entry ${e.type}"><time>${timestamp(e.tick)}</time><span>${escapeHtml(e.message)}</span></div>`).join('');
      $('fullBadge').textContent=this.double?'RACE CONDITION DETECTED':this.sync?'SYNC ON':'SYNC OFF';$('fullBadge').className=`badge ${this.double?'badge-red':this.sync?'badge-green':'badge-red'}`;
      renderPseudo('producerCode','consumerCode');
    }
  }
  function escapeHtml(value){return String(value).replace(/[&<>"']/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));}
  function renderPseudo(producerId,consumerId) {
    const p=fullState?.agents.find(a=>a.status==='ready')?.phase||'wait-empty';const c=fullState?.consumers.find(a=>a.status==='ready')?.phase||'wait-full';
    const producerHighlight=({ 'wait-empty':0,'wait-mutex':1,insert:2,'signal-mutex':4,'signal-full':5 })[p];
    const consumerHighlight=({ 'wait-full':0,'wait-mutex':1,remove:2,'signal-mutex':4,'signal-empty':5,'lock-seat':6,'read-counter':7,'read-seat':7,'write-seat':8,'release-seat':9 })[c];
    $(producerId).innerHTML=PRODUCER_CODE.map((line,i)=>`<div class="code-line${i===producerHighlight?' current':''}">${String(i+1).padStart(2,'0')}  ${line}</div>`).join('');
    $(consumerId).innerHTML=CONSUMER_CODE.map((line,i)=>`<div class="code-line${i===consumerHighlight?' current':''}">${String(i+1).padStart(2,'0')}  ${line}</div>`).join('');
  }

  function switchMode(mode) {
    activeMode=mode;
    document.querySelectorAll('.tab').forEach(tab=>{const on=tab.dataset.mode===mode;tab.classList.toggle('active',on);tab.setAttribute('aria-selected',String(on));});
    document.querySelectorAll('.mode-panel').forEach(panel=>{const on=panel.id===`${mode}Mode`;panel.classList.toggle('hidden',!on);panel.setAttribute('aria-hidden',String(!on));});
  }
  function wireTabs(){document.querySelectorAll('.tab').forEach((tab,i)=>{tab.addEventListener('click',()=>switchMode(tab.dataset.mode));tab.addEventListener('keydown',e=>{if(!['ArrowLeft','ArrowRight'].includes(e.key))return;e.preventDefault();const tabs=[...document.querySelectorAll('.tab')],next=(i+(e.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;tabs[next].focus();tabs[next].click();});});}

  function getFullConfig(sync= $('syncToggle').checked){return {capacity:$('fullCapacity').value,producers:$('producerCount').value,consumers:$('consumerCount').value,producerSpeed:$('producerSpeed').value,consumerSpeed:$('consumerSpeed').value,sync,seed:$('seedInput').value};}
  function updateRangeOutput(){for(const id of ['producerCount','consumerCount','fullCapacity','producerSpeed','consumerSpeed'])$(`${id}Out`).textContent=$(id).value;}
  function updateHeader(){const text=selfCheckStatus|| (fullState?.double?'RACE DETECTED':fullState?.running?'SIMULATION LIVE':'LAB READY');$('headerStatus').lastElementChild.textContent=text;$('headerStatus').classList.toggle('race-header',Boolean(fullState?.double)||Boolean(selfCheckStatus?.includes('FAILURE')));}
  function runFullStep(){if(!fullState)return;fullState.configureFromControls();fullState.doStep();updateHeader();}
  function toggleFullRun(){if(fullState.running){fullState.stop();$('simPause').textContent='▶ Resume';updateHeader();return;}fullState.running=true;$('simPause').textContent='Ⅱ Pause';fullTimer=setInterval(runFullStep,450);updateHeader();}
  function resetFull(){fullState.reset(getFullConfig());$('simPause').textContent='▶ Run';updateHeader();}
  function handleConfigChange(){updateRangeOutput();if(fullState){const countsChanged=Number($('producerCount').value)!==fullState.producerCount||Number($('consumerCount').value)!==fullState.consumerCount;if(Number($('fullCapacity').value)!==fullState.capacity||countsChanged)resetFull();else{fullState.configureFromControls();fullState.render();}}}

  function runChecks() {
    const unsafe=newRace(false);for(let i=0;i<12&&!unsafe.agents.every(a=>a.status==='done');i++)stepRace(unsafe);
    const safe=newRace(true);for(let i=0;i<20&&!safe.agents.every(a=>a.status==='done');i++)stepRace(safe);
    let ok=unsafe.double>0&&safe.double===0&&safe.counter===34;
    const base={capacity:5,producers:2,consumers:2,producerSpeed:5,consumerSpeed:5,seed:204,quiet:true};
    const integratedUnsafe=new FullSimulation({...base,sync:false}),integratedSafe=new FullSimulation({...base,sync:true});
    for(let i=0;i<180;i++){integratedUnsafe.doStep(false);integratedSafe.doStep(false);}
    const safeInvariant=integratedSafe.invariantFailures===0&&integratedSafe.empty.count+integratedSafe.full.count+integratedSafe.reservedEmpty+integratedSafe.reservedFull+integratedSafe.pendingFull+integratedSafe.pendingEmpty===integratedSafe.capacity;
    const oldBuffer=bufferState,oldCapacity=$('bufferCapacity').value;makeBufferMode(3);
    for(let i=0;i<4;i++)bufferProduce();const fullWorked=bufferState.queue.length===3&&bufferState.empty===0&&bufferState.full===3&&bufferState.producerWait.length===1;
    const fifoBefore=bufferState.queue.map(x=>x.id),waitingProducer=bufferState.producerWait[0].id;bufferConsume();const fifoWorked=bufferState.queue.map(x=>x.id).join(',')===`${fifoBefore[1]},${fifoBefore[2]},${waitingProducer}`;
    for(let i=0;i<3;i++)bufferConsume();bufferConsume();const emptyWorked=bufferState.queue.length===0&&bufferState.empty===3&&bufferState.full===0&&bufferState.consumerWait.length===1;
    bufferState=oldBuffer;$('bufferCapacity').value=oldCapacity;renderBufferMode();
    const metrics=[
      {check:'sync OFF reproduces double booking in guided race',pass:unsafe.double>0},
      {check:'sync ON protects guided mutex transaction',pass:safe.double===0&&safe.counter===34},
      {check:'integrated unsafe seed reproduces a double booking',pass:integratedUnsafe.double>0},
      {check:'integrated sync ON has no double booking',pass:integratedSafe.double===0},
      {check:'empty + full + in-flight permits = N at every instruction',pass:safeInvariant},
      {check:'bounded buffer blocks at full and wakes producer FIFO',pass:fullWorked&&fifoWorked},
      {check:'bounded buffer blocks consumer at empty',pass:emptyWorked},
    ];
    ok=ok&&metrics.every(x=>x.pass);console.table(metrics);$('checkSummary').textContent=metrics.map(x=>`${x.pass?'PASS':'FAIL'} · ${x.check}`).join('  |  ');
    selfCheckStatus=ok?'SELF-CHECKS PASSED':'SELF-CHECK FAILURE';$('headerStatus').lastElementChild.textContent=selfCheckStatus;$('headerStatus').classList.toggle('race-header',!ok);
    $('summary-card')?.scrollIntoView?.({behavior:'smooth',block:'center'});
  }

  function initialize() {
    wireTabs();
    $('raceStep').addEventListener('click',()=>stepRace(raceState));$('raceAuto').addEventListener('click',()=>autoRace(false));$('raceReset').addEventListener('click',()=>{newRace(false);renderRace(raceState);});
    $('mutexStep').addEventListener('click',()=>stepRace(mutexState));$('mutexAuto').addEventListener('click',()=>autoRace(true));$('mutexReset').addEventListener('click',()=>{newRace(true);renderRace(mutexState);});
    newRace(false);renderRace(raceState);newRace(true);renderRace(mutexState);
    $('raceCode').innerHTML=RACE_LINES.map((line,i)=>`<div class="code-line${i===0?' current':''}${i>=2?' error':''}">${String(i+1).padStart(2,'0')}  ${line}</div>`).join('');
    $('mutexCode').innerHTML=['wait(mutex)',...RACE_LINES,'signal(mutex)'].map((line,i)=>`<div class="code-line${i===0?' current':''}">${String(i+1).padStart(2,'0')}  ${line}</div>`).join('');
    $('bufferCapacity').addEventListener('change',e=>makeBufferMode(Number(e.target.value)));$('produceBtn').addEventListener('click',bufferProduce);$('consumeBtn').addEventListener('click',bufferConsume);$('bufferReset').addEventListener('click',()=>makeBufferMode(Number($('bufferCapacity').value)));makeBufferMode(5);
    updateRangeOutput();fullState=new FullSimulation(getFullConfig());
    for(const id of ['producerCount','consumerCount','fullCapacity','producerSpeed','consumerSpeed'])$(id).addEventListener('input',handleConfigChange);
    $('syncToggle').addEventListener('change',e=>{fullState.configureFromControls();fullState.setSync(e.target.checked);$('syncToggleLabel').textContent=e.target.checked?'ON':'OFF';updateHeader();});
    $('simPause').addEventListener('click',toggleFullRun);$('simStep').addEventListener('click',runFullStep);$('simReset').addEventListener('click',resetFull);$('seedInput').addEventListener('change',resetFull);$('runChecks').addEventListener('click',runChecks);
    updateHeader();
    if(new URLSearchParams(location.search).get('selfcheck')==='1')window.setTimeout(runChecks,100);
  }
  document.addEventListener('DOMContentLoaded',initialize);
})();
