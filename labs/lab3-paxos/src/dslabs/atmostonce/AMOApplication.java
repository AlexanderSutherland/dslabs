package dslabs.atmostonce;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

//Alex's imports
import dslabs.framework.Address;
import java.util.HashMap;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final KVStore application;


  // Tracks the highest sequence number for each client
  private final HashMap<Address, Integer> completedClientSequenceNumber = new HashMap<>();
  private final HashMap<Address, AMOResult> previousClientResult = new HashMap<>();


  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand amoCommand)) {
      throw new IllegalArgumentException();
    }

      // Check if client exists in seen clients
    completedClientSequenceNumber.putIfAbsent(amoCommand.senderAddress(), -1);


    // Grab sequence number from client
    int givenSequenceNumber = amoCommand.sequenceNum();


    // Check if the command has already been executed
    if (alreadyExecuted(amoCommand)) {

      // Case where it's cached
      if (givenSequenceNumber == completedClientSequenceNumber.get(amoCommand.senderAddress())) {
        return previousClientResult.get(amoCommand.senderAddress());
      }
      // Return nothing if item is old request that the client has already received
      return null;
    }


    // Execute the command using the underlying application
    Result appResult = application.execute(amoCommand.command());

    // Update Sequence Numbers responded to
    completedClientSequenceNumber.put(amoCommand.senderAddress(), givenSequenceNumber);

    // Create the AMOResult with the application result and sequence number
    AMOResult amoResult = new AMOResult(appResult, givenSequenceNumber);

    // Cache the result
    previousClientResult.put(amoCommand.senderAddress(), amoResult);

    return amoResult;

  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    Integer completedSeqNum = completedClientSequenceNumber.get(amoCommand.senderAddress());
    return completedSeqNum != null && amoCommand.sequenceNum() <= completedSeqNum;
  }
}