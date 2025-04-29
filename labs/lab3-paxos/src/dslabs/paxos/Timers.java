package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Timer;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
//import org.junit.runner.Request;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final PaxosRequest currentRequest;
}

/**
 * Once Acceptors have accepted a new leader,
 * they need to check the leader is still alive
 */
@Data
final class HeartBeatCheckTimer implements Timer {
  static final int HEARTBEAT_CHECK_MILLIS = 100;
  private final PaxosServer.Ballot ballot;
}

/**
 * Sends a Heartbeat message to Acceptors. Timer
 * should only be active while leader
 */
@Data
final class HeartBeatTimer implements Timer {
  static final int HEARTBEAT_MILLIS = 25;
  private final PaxosServer.Ballot ballot;
}
