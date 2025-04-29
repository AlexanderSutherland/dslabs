package dslabs.paxos;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.*;
import dslabs.kvstore.KVStore;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import lombok.extern.java.Log;

import java.io.Serializable;
import java.util.*;

import dslabs.atmostonce.AMOApplication;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {

  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  /** Application */
  private AMOApplication<Application> app;

  // for sub node constructor
  private final Address rootAddress;

  /** Current Ballot, also can be seen as current view */
  private Ballot currentBallot;

  /** Phase 1 Leader Info
   * HashSet needed in case network duplicates P1B msg from a server
   */
  private HashSet<Address> votePhase1Set = null;
  private boolean votePhase1Majority = false;

  /**
   * Phase 2 Leader Info: This map tracks the number of acceptors for each proposal
   * since there can be multiple proposals at a time.
   */
  private Multimap<Integer, Address> acceptanceMultiMap;


  /** Track Leader heartbeats */
  private Integer receivedHeartBeats;

  /** Map representing the slots, keys are int and values are Log objects */
  Map<Integer, LogEntry> paxosLogs;

  /** Pointer to the next available (empty) slot */
  private Integer slotIn;

  /** Time that a server will wait after attempting to be leader or accepting a ballot for a new leader*/

  private final int COOLDOWN_ROUNDS = 1;
  private int cooldownCounter = 0;

  /** Pointers to the first slot that hasn't been decided/executed yet */
  private Map<Address, Integer> slotOutMap;

  private HeartBeatCheckTimer heartBeatCheckTimer = null;

  /** Pointer to the first slot that hasn't been garbage collected */
  private Integer slotTrim;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    // Create a new application
    this.app = new AMOApplication<>(app);

    // No given rootAddress so set to Null
    this.rootAddress = null;
  }

  public PaxosServer(Address address, Address[] servers, Address rootAddress) {
    super(address);
    this.servers = servers;

    // No given Application but rootAddress has Application
    this.app = null;

    // Set rootAddress
    this.rootAddress = rootAddress;
  }

  @Override
  public void init() {

    // Initialize PaxosLog Info
    paxosLogs = new HashMap<>();
    acceptanceMultiMap = HashMultimap.create();

    // Initialize hashset for phase 1 votes. Needed in the case
    votePhase1Set = new HashSet<>();

    slotIn = 1;
    slotOutMap = new HashMap<>();
    slotTrim = 1;

    // Keep track of all server slotOuts
    for (Address server : servers) {
      slotOutMap.put(server, 1);
    }

    // Initialize ballot but will be updated by initiateLeadershipElection
    currentBallot = new Ballot(0, null);

    initiateLeadershipElection();

    receivedHeartBeats = 0;

    heartBeatCheckTimer = new HeartBeatCheckTimer(currentBallot);
    set(heartBeatCheckTimer, HeartBeatCheckTimer.HEARTBEAT_CHECK_MILLIS);

  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
      // Case before slot_trim so should not exist
    if (logSlotNum < slotTrim) return PaxosLogSlotStatus.CLEARED;
      // Return status if slotNum exists
    else if (paxosLogs.containsKey(logSlotNum)) return paxosLogs.get(logSlotNum).status();
    // Should be Empty otherwise
    return PaxosLogSlotStatus.EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Needs to contain the key and the key can't be a No-Op
    if (paxosLogs.containsKey(logSlotNum) && paxosLogs.get(logSlotNum).amoCommand() != null) {
      return paxosLogs.get(logSlotNum).amoCommand().command();
    } else {
      return null;
    }
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    return slotTrim;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    return slotIn - 1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers for Phase 1
   * ---------------------------------------------------------------------------------------------*/
  /**
   * Handles a Phase 1A message in the Paxos protocol.
   * <p>
   * This method is called when a PaxosServer initiates Phase 1 of Paxos by sending
   * a Phase 1A message. If the received ballot number is higher than the current
   * ballot, the server accepts the leader and updates its ballot and starts a
   * heartbeat check timer for the new leader. The server then responds with a
   * Phase 1B message, including its current ballot and Paxos log entries.
   *
   * @param m      The Phase 1A message containing the proposer's ballot number.
   * @param sender The address of the proposer sending the message.
   */
  private void handlePhase1A(Phase1A m, Address sender) {

    // If Proposer ballot is higher than currently held. Update ballot
    if (currentBallot.compareTo(m.ballot()) < 0) {
      updateBallotView(m.ballot());
    }

    // Case where a server's msg p1a got delayed but another server is leader now and already got a p1b response from first server
    if (currentBallot.ballotAddress.equals(this.address()) && votePhase1Majority && votePhase1Set.contains(sender)) {
      return;
    }


    // Send response regardless if accepted leader or rejected leader
    send(new Phase1B(currentBallot, paxosLogs, slotOutMap), sender);
  }

  /**
   * Handles a Phase 1B message
   * <p>
   * This method processes responses from acceptors during Phase 1 of PMMC. It tracks
   * votes from acceptors who acknowledge this server as the leader. If a majority
   * is reached, the server assumes leadership and begins sending heartbeat messages.
   * If a higher ballot number is encountered, the server updates its state accordingly.
   * Additionally, it processes accepted and chosen log entries to maintain consistency.
   *
   * @param m      The Phase 1B message containing the acceptor’s response and log entries.
   * @param sender The address of the acceptor sending the message.
   */
  private void handlePhase1B(Phase1B m, Address sender) {


    if (currentBallot.compareTo(m.ballot()) < 0) {
      updateBallotView(m.ballot());
      return;
    }

    // Case where Acceptor has accepted this PaxosServer as leader
    if (currentBallot.compareTo(m.ballot()) == 0 && currentBallot.ballotAddress == address()) {
      votePhase1Set.add(sender);

      // Update Accepted Log entries
      mergeLogs(m.acceptorLogEntries());

      // Case where Leader has gotten majority and can start sending a heart beat
      // Only want to hit this if then once which is why votePhase1Majority is needed
      if (votePhase1Majority)  updateLogs();

      if (votePhase1Set.size() >= (servers.length / 2) && !votePhase1Majority) {


        LOG.finer(String.format("Server %s accepted as leader.", address()));
        votePhase1Majority = true;

        // Need to reset acceptance Map in case Server was a leader prior
        acceptanceMultiMap.clear();

        // Doing this before the heart beat in order to update accepted entry ballot to this leader's ballot
        updateLogs();
        set(new HeartBeatTimer(currentBallot), HeartBeatTimer.HEARTBEAT_MILLIS);
        heartBeatCheckTimer = null;

        runGarbageCollection(m.slotOutMap());

      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers for Phase 2
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Handler for a paxos request received from the client
   *
   * @param m The message from the client
   * @param sender The address of the client
   */
  private void handlePaxosRequest(PaxosRequest m, Address sender) {

    // handle readonly request
    if (m.amoCommand().readOnly()) {
      if (this.app != null) {
        // TODO not sure if sequence numbers will have any issue with readonly commands
        // Alternative Commented below
        //Result result = app.executeReadOnly(m.amoCommand().command());
        //AMOResult amoResult = new AMOResult(result, m.amoCommand().sequenceNum());
        AMOResult amoResult = (AMOResult) app.executeReadOnly(m.amoCommand());
        send(new PaxosReply(amoResult), sender);
      }
      return;
    }


    // If Command is already executed immediately return to client
    if (app.alreadyExecuted(m.amoCommand())){
      sendRequestReply(m.amoCommand(), sender);
      LOG.finer(String.format("Already Executed, returning to client %s", sender));
    }

    // Only leader should respond to client request. End the handler
    else if ( !currentBallot.ballotAddress.equals(this.address()) || !votePhase1Majority) return;

      // if the command is not already executed and not being processed
      // and server is the leader, assign and slot and initiate the voting
      // procedure. Multiple voting procedures can occur at a time
    else if (findSlotGivenCommand(m.amoCommand(), paxosLogs) == null) {

      // Create new phase 2a request and send out to all the servers
      acceptanceMultiMap.put(slotIn, this.address());
      paxosLogs.put(slotIn, new LogEntry(currentBallot, PaxosLogSlotStatus.ACCEPTED, m.amoCommand()));

      // Case where just single server (Test 27) and will never get P2B and can immediately respond
      if (servers.length == 1) {
        // Slot is chosen and decided for
        paxosLogs.get(slotIn).status(PaxosLogSlotStatus.CHOSEN);
        // Need to execute logs in order

      } else {

        // Create new phase 2a request and send out to all the servers
        broadcast(new Phase2A(currentBallot, slotIn, paxosLogs.get(slotIn)), servers);
      }
      slotIn++;
      executeCommands(true);
    }

    // client is resending a request that has not been executed yet
    else {
      LOG.finer(String.format("Sever %s: Something went wrong: SlotOutMap: %s, SlotIn %s, SlotTrim %s, Slot Number %s, Paxos Log %s", address(), slotOutMap, slotIn, slotTrim, findSlotGivenCommand(m.amoCommand(), paxosLogs), paxosLogs.get(findSlotGivenCommand(m.amoCommand(), paxosLogs))));
    }
  }

  /**
   * Handles a Phase 2A message in the Paxos made moderately complex.
   * It compares the ballots to ensure the leader has not yet been preempted, Then
   * the server adds the log entry to its log at the specified slot. If it puts
   * the log in the slot pointed at by slotIn, it increments slotIn.
   * The server sends a phase 2b message to the leader.
   *
   * @param m      The Phase 2A message containing the proposed value.
   * @param sender The address of the leader sending the message.
   */
  private void handlePhase2A(Phase2A m, Address sender) {
    // don't respond if logEntry ballot is old or if the received ballet is old
    if (currentBallot.compareTo(m.logEntry().ballot()) > 0 || currentBallot.compareTo(m.ballot()) > 0) return;

    // Leader can only send 2A's once it's gotten the majority so this Acceptor can update its ballot view.
    if (currentBallot.compareTo(m.ballot()) < 0) updateBallotView(m.logEntry().ballot());


    // If this message is for an old message we garbage-collected, ignore it
    if (m.slotNum() < slotTrim) return;

    // Need to make sure slotIn is the most front slot of the PaxosLogs
    if (slotIn <= m.slotNum()) slotIn = m.slotNum() + 1;

    if (!paxosLogs.containsKey(m.slotNum()) || paxosLogs.get(m.slotNum()).status != PaxosLogSlotStatus.CHOSEN) {
      paxosLogs.put(m.slotNum(), new LogEntry(m.ballot(), PaxosLogSlotStatus.ACCEPTED, m.logEntry().amoCommand()));
    }

    send(new Phase2B(m.slotNum(), paxosLogs.get(m.slotNum()), slotOutMap), sender);
  }

  /**
   * Handles a Phase 2B message in Paxos made moderately complex
   * When the leader receives a phase 2b message, it increments the number of acceptors
   * that have accepted the message. Once it receives over half of the messages,
   * it changes the value to accepted and sends a decision message out to all the
   * servers. Additionally, it will attempt to execute all un-executed commands, moving
   * through the list until it reaches one that is not yet chosen.
   *
   * @param m      The Phase 2B message indicating acceptance of a proposal.
   * @param sender The address of the acceptor sending the message.
   */
  private void handlePhase2B(Phase2B m, Address sender) {

    // if the slot number doesn't exist, ignore it
    if (!paxosLogs.containsKey(m.slotNum())) return;

    // if the value is already chosen it means we got a majority, drop message
    if (Objects.equals(paxosLogs.get(m.slotNum()).status(), PaxosLogSlotStatus.CHOSEN)) return;

    // put the sender in the acceptance map for the slot
    if (m.logEntry().ballot().equals(currentBallot)) {
      acceptanceMultiMap.put(m.slotNum(), sender);
    }

    // once we have a majority of acceptors, we can set the slot to chosen and execute the
    // command if there are no missing values
    if (acceptanceMultiMap.get(m.slotNum()).size() >= servers.length / 2 + 1 && paxosLogs.get(m.slotNum()).status() != PaxosLogSlotStatus.CHOSEN) {

      // Slot is chosen and decided for
      paxosLogs.get(m.slotNum()).status(PaxosLogSlotStatus.CHOSEN);
      //LOG.finer(String.format("Leader %s changing log %s status to CHOSEN", address(), paxosLogs.get(m.slotNum())));

      broadcast(new Decision(m.slotNum(), paxosLogs.get(m.slotNum())), servers);

      executeCommands(true);
    }

    runGarbageCollection(m.slotOutMap());
  }


  /**
   * Handles a decision message in the Paxos protocol.
   * When a server receives a decision message, it means that the value has been
   * decided on and committed. The server will put the log entry from the message
   * in the agreed-upon slot. Next it will attempt to execute the logs starting
   * from the first log that is un-executed but still committed so that the logs
   * will be guaranteed to be executed in the same order. It will continue executing
   * logs until it either runs out of logs or it reaches an uncommitted log (accepted,
   * empty, or cleared).
   *
   * @param m      The decision message containing the agreed-upon log entry.
   * @param sender The address of the sender who sent the decision message.
   */
  void handleDecision(Decision m, Address sender) {

    // Update local ballot if from a newer leader
    if (currentBallot.compareTo(m.logEntry().ballot()) < 0) updateBallotView(m.logEntry().ballot());

    // Check if the Ballots are the same and if so, update slot to chosen
    if (currentBallot.equals(m.logEntry().ballot)) {

      // if we have garbage collected the message already, do not add to the log
      if (m.slotNum() < slotTrim) return;

      paxosLogs.put(m.slotNum(), m.logEntry());

      // Case where this PaxosServer never got the P2A msg
      if (slotIn <= m.slotNum()) slotIn = m.slotNum() + 1;

      // Will execute logs in order until there is a non-chosen slot or it reaches the slot_in marker
      executeCommands(false);
    }
  }

  /**
   * Handles an incoming heartbeat message from the leader.
   * <p>
   * This method logs the received heartbeat message and checks if it matches the
   * current leader's ballot. If the heartbeat is from the expected leader, it resets
   * the missed heartbeat counter to ensure the server does not mistakenly attempt
   * to assume leadership.
   *
   * @param m      The received heartbeat message.
   * @param sender The address of the server that sent the heartbeat.
   */
  private void handleHeartBeat(HeartBeat m, Address sender) {

    runGarbageCollection(m.slotOutMap());

    if (m.slotOutMap().get(this.address()) < slotOutMap.get(this.address())) {
      send(new SlotOutMapMsg(slotOutMap), sender);
    }

    // Need to update leader if newer than what is the current leader
    if (m.leaderBallot().compareTo(currentBallot) > 0) {
      updateBallotView(m.leaderBallot());
    }

    // if the ballots are equal increment the heartbeat and merge the leaders logs
    if (m.leaderBallot().compareTo(currentBallot) == 0) {
      receivedHeartBeats++;
      mergeLogs(m.logEntries());
    }
  }

  private void handleSlotOutMapMsg(SlotOutMapMsg m, Address sender){
    runGarbageCollection(m.slotOutMap());
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Handles the heartbeat timer event (only for leader servers).
   * <p>
   * This method is triggered when the heartbeat timer expires. If the current server is the leader,
   * it sends a heartbeat message to all other servers to indicate its presence and maintain leadership.
   * If the server is no longer the leader, it exits early without sending a heartbeat.
   *
   * @param timer The expired HeartBeatTimer instance triggering this method.
   */
  private void onHeartBeatTimer(HeartBeatTimer timer) {

    // reset heartbeat

    if (Objects.equals(currentBallot, timer.ballot())) {
      receivedHeartBeats = 0;

      for (int slot = slotOut(); slot < slotIn; slot++) {

        // Resend Accepted Commands not yet reached majority
        if (paxosLogs.get(slot).status == PaxosLogSlotStatus.ACCEPTED) {
          for (Address address : servers) {
            // send a p2a request only to servers that have not yet responded
            if (!acceptanceMultiMap.get(slot).contains(address) && !address.equals(address())) {
              paxosLogs.get(slot).ballot(currentBallot);
              send(new Phase2A(currentBallot, slot, paxosLogs.get(slot)), address);
            }
          }
        }
      }

      // Grab all logs
      broadcast(new HeartBeat(currentBallot, paxosLogs, slotOutMap), servers);
      set(new HeartBeatTimer(currentBallot), HeartBeatTimer.HEARTBEAT_MILLIS);
    }
  }

  /**
   * Handles the heartbeat check timer event.
   * <p>
   * This method is triggered periodically to check if heartbeats from the current leader
   * have been missed. If the number of missed heartbeats reaches or exceeds a threshold,
   * this server attempts to become the leader by incrementing its ballot number and
   * broadcasting a Phase 1A message to other Paxos servers. It then resets the heartbeat
   * counter and starts a new heartbeat timer.
   *
   * @param timer The heartbeat check timer that triggered this event.
   */
  private void onHeartBeatCheckTimer(HeartBeatCheckTimer timer) {

    // wait for a certain amount of time after accepting a new leader
    if (timer.equals(heartBeatCheckTimer)) {

      if (cooldownCounter == 0) {

        // Try to become leader if not received heartbeat
        if (receivedHeartBeats == 0) {
          initiateLeadershipElection();
        }
      }

      else cooldownCounter--;

      receivedHeartBeats = 0;

      // Reset timer
      heartBeatCheckTimer = new HeartBeatCheckTimer(currentBallot);
      set(heartBeatCheckTimer, HeartBeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Finds if the given AMOCommand is present in any log entry in PaxosLogs
   * and returns the slot num
   *
   * @param command the command to check.
   * @return Integer if the command is found in any log entry, null otherwise.
   */
  private Integer findSlotGivenCommand(AMOCommand command, Map<Integer, LogEntry> logs) {

    // Loop through slots to see Command has been added to logs
    for (Integer slotNum : logs.keySet()) {
      LogEntry logEntry = logs.get(slotNum);
      if (Objects.equals(logEntry.amoCommand(), command)) return slotNum;
    }
    // No log contains the command return null
    return null;
  }

  /**
   * Merges the logs from an acceptor into the leader's Paxos log.
   * <p>
   * This method ensures log consistency by incorporating entries from the acceptor's logs
   * while maintaining order and correctness. It handles cases where:
   * - The acceptor has no logs.
   * - The logs are already identical.
   * - Entries need to be added to the leader's log.
   * - Chosen values must be respected and executed in order.
   * - Accepted logs from the acceptor with a higher ballot should overwrite older leader logs.
   * <p>
   * This function helps maintain consistency across Paxos replicas while ensuring correctness
   * in garbage collection and execution ordering.
   *
   * @param senderLogs The logs received from the sender (leader for heartbeats, acceptors for Phase1B's).
   */
  void mergeLogs(Map<Integer, LogEntry> senderLogs) {

    // Case where the Sender has no Entries
    if (senderLogs.isEmpty() || senderLogs.equals(paxosLogs)) return;


    for (Integer slotNum : senderLogs.keySet()) {
      LogEntry senderLogEntry = senderLogs.get(slotNum);

      // If a server sends logs that have been cleared, ignore them
      if (slotNum < slotTrim) {
        continue;
      }

      // AMOCommand does not exist in PaxosServer
      if (slotNum >= slotIn){
        // Chosen need to be in order

        // Need to make sure slotIn is the most front slot of the PaxosLogs
        slotIn = slotNum + 1;

        paxosLogs.put(slotNum, senderLogEntry);

      }

      // if slotNum is less than slotIn
      else {

        // Case where in between slotOut and slotIn, the entry does not exist
        if (paxosLogs.get(slotNum) == null) {
          paxosLogs.put(slotNum, senderLogEntry);

        } else {

          LogEntry localLogEntry = paxosLogs.get(slotNum);

          // Case where Sender's Entry is CHOSEN
          if (Objects.equals(senderLogEntry.status(), PaxosLogSlotStatus.CHOSEN)) {
            paxosLogs.put(slotNum, senderLogEntry);

            // Increment slotOut if slotNum is equal since slotOut is left most non-executed slot
            if (slotNum.equals(slotOut())) {
              if (paxosLogs.get(slotOut()).amoCommand != null) app.execute(paxosLogs.get(slotOut()).amoCommand);
              incrementSlotOut();
            }


            // slotOut always needs to be equal or less than slotIn
            if (slotOut() > slotIn) slotIn = slotOut();
          }
          // Case where leader has older log accepted log.
          // NOT COMPARING Leader Ballot, COMPARING Ballot in LogEntry. Should be updated when P2A is sent
          else if (Objects.equals(localLogEntry.status(), PaxosLogSlotStatus.ACCEPTED)
                  && localLogEntry.ballot().compareTo(senderLogEntry.ballot()) < 0) {
            paxosLogs.put(slotNum, senderLogEntry);

          }
        }
      }

    } // end for
    executeCommands(false);
  }

  /**
   * Updates and broadcasts accepted logs in the Paxos leader.
   * <p>
   * This method iterates through the Paxos logs in sorted order, skipping
   * decided, chosen, or garbage slots. For each log entry with "ACCEPTED" status,
   * it updates the entry with the leader’s current ballot
   */
  private void updateLogs() {

    // for each accepted slot in the log, update the ballot
    for (Integer slotNum : paxosLogs.keySet()) {
      LogEntry logEntry = paxosLogs.get(slotNum);

      // if the status is accepted
      if (logEntry.status() == PaxosLogSlotStatus.ACCEPTED) {
        // Update logEntry with leader ballot
        logEntry.ballot(currentBallot);

        // Update Acceptance map with the leader address
        acceptanceMultiMap.get(slotNum).add(this.address());
      }
    }

    for (int slot = slotOut(); slot < slotIn; slot++) {

      // Fill Empty gaps with a No-Op Command in these rare cases. Usually takes many runs to see this happen
      if (paxosLogs.get(slot) == null) {
        paxosLogs.put(slot, new LogEntry(currentBallot, PaxosLogSlotStatus.ACCEPTED, null));

        // Update Acceptance map with the leader address
        acceptanceMultiMap.get(slot).add(this.address());
      }
    }
  }


  /**
   *
   * @param slotOutMapInput the map from the sender
   */
  private void runGarbageCollection(Map<Address, Integer> slotOutMapInput) {

    // for every entry in the received slotout map, update the local slotout map
    for (Address slotOut : slotOutMapInput.keySet()) {

      // if the input map has a greater slot out than the current one, replace it
      if (slotOutMapInput.get(slotOut) > slotOutMap.get(slotOut)) {
        slotOutMap.put(slotOut, slotOutMapInput.get(slotOut));
      }
    }

    // get the smallest slot out from the map
    int furthestBehindSlotOut = Collections.min(slotOutMap.values());

    // delete logs until the slotTrim is equal to the furthest behind slotOut
    while (slotTrim < furthestBehindSlotOut) {
      paxosLogs.remove(slotTrim);
      acceptanceMultiMap.removeAll(slotTrim);
      slotTrim++;
    }
  }

  /**
   * Updates the current ballot view with the given ballot.
   * <p>
   * This method replaces the current ballot with the provided one and resets
   * phase 1 voting state. Resetting phase 1 voting preventing non-leaders
   * from trying to perform leader actions. It also schedules a heartbeat
   * check timer to monitor the new ballot's status.
   *
   * @param ballot the new ballot to update the view with
   */
  private void updateBallotView(Ballot ballot) {
    currentBallot = ballot;
    votePhase1Majority = false;
    receivedHeartBeats = 0;
    cooldownCounter = COOLDOWN_ROUNDS;

    // if the timer isn't set and we are not the leader, set the heartbeat check timer
    if (heartBeatCheckTimer == null && !currentBallot.ballotAddress().equals(address())) {
      heartBeatCheckTimer = new HeartBeatCheckTimer(currentBallot);
      set(heartBeatCheckTimer, HeartBeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
    }
  }

  /**
   * Initiates a leadership election for this server in the Paxos consensus algorithm.
   * <p>
   * This method increments the current ballot number, sets this server as the leader,
   * and resets the missed heartbeat counter. It also starts the election process by
   * broadcasting a Phase 1A (prepare) message to other Paxos servers.
   */
  private void initiateLeadershipElection(){
    // Update ballot to make this server the leader


    if(currentBallot.ballotAddress == null ||
            !currentBallot.ballotAddress.equals(address())){

      currentBallot.ballotNum++;
      currentBallot.ballotAddress(this.address());
      votePhase1Majority = false;
      votePhase1Set.clear(); //Need to reset votes

      receivedHeartBeats = 0;

      if (servers.length == 1){
        votePhase1Majority = true;
      }
    }

    if (!votePhase1Majority) {
      // Broadcast p1a and heartbeat to other paxos servers
      LOG.finer(String.format("Server %s attempting to be leader", address()));
      broadcast(new Phase1A(currentBallot), servers);
    }
  }


  /**
   * Executes and replies to Paxos requests in order.
   * <p>
   * This method ensures that logs are executed sequentially without skipping any slot.
   * It processes logs only if they are marked as CHOSEN. If a command exists, it is executed,
   * and the result is sent back to the sender. The slotOut pointer is incremented after execution.
   * <p>
   * Preconditions:
   * - Logs must be executed in order (no out-of-order execution).
   * - Only logs with status CHOSEN will be processed.
   * <p>
   * Side Effects:
   * - Executes commands stored in the Paxos log.
   * - Sends replies to the corresponding requesters.
   * - Advances the slotOut counter after processing.
   */
  private void executeCommands(Boolean isLeader){
    // Need to execute logs in order. Can not be out of order. Hence, the use of slot out
    while (slotOut() < slotIn
            && paxosLogs.get(slotOut()) != null
            && paxosLogs.get(slotOut()).status == PaxosLogSlotStatus.CHOSEN) {
      //AMOResult result = null;
      //if (paxosLogs.get(slotOut()).amoCommand != null) result = app.execute(paxosLogs.get(slotOut()).amoCommand);
      //if (result != null && isLeader) send(new PaxosReply(result), paxosLogs.get(slotOut()).amoCommand.senderAddress());
      if (paxosLogs.get(slotOut()).amoCommand != null && isLeader) sendRequestReply(paxosLogs.get(slotOut()).amoCommand, paxosLogs.get(slotOut()).amoCommand.senderAddress());


      incrementSlotOut();
    }
  }


  /**
   * Nodes should never send messages to themselves. This method overrides the broadcast method to
   * send messages to all nodes but the current node.
   *
   * @param message The message to broadcast
   * @param addresses The list of addresses to broadcast to
   */
  @Override
  protected void broadcast(Message message, Address[] addresses){
    for (Address address : addresses) {
      if (address.equals(this.address())) continue;
      send(message, address);
    }
  }


  // Retrieves the current slotOut value for this server
  private Integer slotOut(){return slotOutMap.get(this.address());}

  // Increments the slotOut value for this server
  private void incrementSlotOut(){slotOutMap.put(this.address(), slotOutMap.get(this.address())+1);}


  private void sendRequestReply(AMOCommand amoCommand, Address sender){
    if (this.app != null) {
      AMOResult amoResult = app.execute(amoCommand);
      if (amoResult != null) {send(new PaxosReply(amoResult), sender);}
    } else {
      assert this.rootAddress != null : "Sending Decision with Root Address AS NULL";
      this.handleMessage(new PaxosDecision(amoCommand), this.rootAddress);
    }
  }

  /*
  ----------------------------------------------------------------------

    Inner Classes

  ----------------------------------------------------------------------
  */

  /**
   * Represents a log entry in the Paxos consensus protocol.
   * <p>
   * Each log entry contains a ballot identifier, status, and an at most once
   * (AMO) command. This structure is used to keep track of decisions made during
   * the consensus process.
   * <p>
   * - `ballot`: A unique identifier for the ballot associated with this log entry
   * - `status`: The current status of this log entry in the Paxos log
   * - `amoCommand`: The command associated with this log entry, representing an
   *   operation to be executed once consensus is reached.
   * <p>
   * This class implements `Serializable` to allow persistence and transmission
   * of log entries across different nodes in the system.
   */
  @Data
  @EqualsAndHashCode
  @AllArgsConstructor
  static class LogEntry implements Serializable {
    private Ballot ballot;
    private PaxosLogSlotStatus status;
    private AMOCommand amoCommand;
  }

  /**
   * Represents a ballot in the Paxos consensus algorithm.
   * Each ballot consists of a ballot number and the address of the server that proposed it.
   * <p>
   * **Ballot Address:** If two ballots have the same number, the one with the lower address is ranked higher.
   *    (This is achieved by negating the result of the address comparison.)
   * <p>
   * This ensures that (1, server1) > (1, server2) when server1 has a lower address than server2.
   */
  @Data
  @EqualsAndHashCode
  @AllArgsConstructor
  static class Ballot implements Serializable, Comparable<Ballot> {
    private Integer ballotNum;
    private Address ballotAddress;

    // Need to override in order for (1, server1) < (1, server2)
    @Override
    public int compareTo(Ballot otherBallot) {

      // Compare ballotNum first
      int numComparison = this.ballotNum.compareTo(otherBallot.ballotNum);
      if (numComparison != 0) return numComparison;

      // Same ballotNum but lower server results in higher compare
      return this.ballotAddress.compareTo(otherBallot.ballotAddress);
    }
    @Override
    public String toString(){
      return "(Number: " + ballotNum + ", Address: " + ballotAddress + ")";
    }
  }

}