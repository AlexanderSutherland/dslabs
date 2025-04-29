package dslabs.paxos;
// Your code here...

import dslabs.framework.Address;
import dslabs.framework.Message;
import lombok.Data;
import dslabs.paxos.PaxosServer.LogEntry;
import dslabs.paxos.PaxosServer.Ballot;

import java.util.Map;


/* -----------------------------------------------------------------------------------------------
 *  Phase 1 Messages: Messages for determining leader
 * ---------------------------------------------------------------------------------------------*/

/**
 * Represents a p1a (Phase1A) message in the Paxos protocol.
 *
 * A proposer sends a p1a message to acceptors to initiate a new ballot
 * in the first phase (Phase 1) of Paxos. The ballot is a unique identifier
 * consisting of a round number and the proposer's address.
 */
@Data
class Phase1A implements Message {
    private final Ballot ballot;
}


/**
 * Represents a Promise message in the Paxos protocol.
 *
 * After receiving a p1a (Phase1A) message, an acceptor responds with a
 * Promise message (p1b) if it has not already promised a higher ballot.
 * This confirms that the acceptor will not accept proposals from
 * lower ballot numbers in the future.
 */
@Data
class Phase1B implements Message {
    private final Ballot ballot;
    private final Map<Integer, LogEntry> acceptorLogEntries;
    private final Map<Address, Integer> slotOutMap;
}


@Data
class Phase2A implements Message {
    private final Ballot ballot;
    private final Integer slotNum;
    private final LogEntry logEntry;
}

@Data
class Phase2B implements Message {
    private final Integer slotNum;
    private final LogEntry logEntry;
    private final Map<Address, Integer> slotOutMap;
}

@Data
class Decision implements Message {
    private final Integer slotNum;
    private final LogEntry logEntry;
}


/* -----------------------------------------------------------------------------------------------
 *  Heart beat Message
 * ---------------------------------------------------------------------------------------------*/

/**
 * Sync up with Heartbeat message
 */
@Data
class HeartBeat implements Message {
    // Ballot that got elected
    private final Ballot leaderBallot;

    // Leader logs for acceptors to catch up with
    private final Map<Integer, LogEntry> logEntries;

    // Slowest allow slotTrim to hold (Leader needs to keep track of acceptors slotOut to update slotTrim)
    private final Map<Address, Integer> slotOutMap;
}

@Data
class SlotOutMapMsg implements Message {
    private final Map<Address, Integer> slotOutMap;
}