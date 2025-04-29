package dslabs.atmostonce;

import dslabs.framework.Command;
import lombok.Data;

//Alex's imports
import dslabs.framework.Address;

/**
 * AMOCommand is used to shim the result of an application command with
 * a sequence number to ensure at-most-once semantics.
 *
 * This Request structure helps maintain the order of Requests, ensuring
 * that duplicate commands are not processed more than once.
 */
@Data
public final class AMOCommand implements Command {
    private final Command command;
    private final int sequenceNum;
    private final Address senderAddress;

    @Override
    public String toString(){
        String cmdStr = command.toString();
        String cappedCmd = cmdStr.length() > 200 ? cmdStr.substring(0, 200) + "..." : cmdStr;
        return "( Addr:" + senderAddress + ", seqNum:" + sequenceNum + ", cmd:" + cappedCmd + ")";
    }
}