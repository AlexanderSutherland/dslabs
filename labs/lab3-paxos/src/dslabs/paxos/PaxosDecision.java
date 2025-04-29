package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

/**
 * Please see {@link PaxosRequest} for illustration.
 */

@Data
public class PaxosDecision implements Message {
    private final AMOCommand amoCommand;
}
