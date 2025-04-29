package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.java.Log;

import java.util.Objects;

import static dslabs.paxos.ClientTimer.CLIENT_RETRY_MILLIS;

@Log
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...
  private PaxosRequest currentRequest;
  private PaxosReply paxosReply;
  private int incrementSeqNum() {
    if (currentRequest == null) {
      return 0;
    }
    return currentRequest.amoCommand().sequenceNum() + 1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    if (!(operation instanceof KVStore.KVStoreCommand)) {
      throw new IllegalArgumentException();
    }
    currentRequest = new PaxosRequest(new AMOCommand(operation, incrementSeqNum(), address()));
    paxosReply = null;


    //LOG.finer(String.format("Client %s sending command %s", address(), operation));
    broadcast(currentRequest, servers);


    set(new ClientTimer(currentRequest), CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return paxosReply != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...

    // blocks until there is a result for the most recent command
    while (paxosReply == null) {
      this.wait();
    }

    //LOG.finer(String.format("Client %s received result %s", address(), paxosReply));
    return paxosReply.amoResult().appResult();
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    if (currentRequest.amoCommand().sequenceNum() == m.amoResult().sequenceNum()) {
      paxosReply = m;
      this.notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (Objects.equals(currentRequest, t.currentRequest()) && paxosReply == null) {
      LOG.finer(String.format("Client %s resending command %s ", address(), t.currentRequest()));
      broadcast(currentRequest, servers);
      set(t, CLIENT_RETRY_MILLIS);
    }
  }
}
