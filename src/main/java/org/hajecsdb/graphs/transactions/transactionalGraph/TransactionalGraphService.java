package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.IdGenerator;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.impl.NodeImpl;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class TransactionalGraphService {

    private Transaction transaction;
    private TGraphImpl tGraph = new TGraphImpl();

    public TGraph context(Transaction transaction) throws TransactionException {
        if (transaction == null)
            throw new TransactionException("Not defined transaction!");

        if (transaction.isPerformed())
            throw new TransactionException("Transaction was performed! [COMMITED OR ROLLBACKED]");

        this.transaction = transaction;
        return tGraph;
    }

    public Set<Node> getAllPersistentNodes() {
        return tGraph.tNodes.stream().filter(tNode -> tNode.isCommitted()).map(tNode -> tNode.getOriginNode()).collect(Collectors.toSet());
    }

    public Optional<Node> getPersistentNodeById(long nodeId) {
        return tGraph.tNodes.stream()
                .filter(tNode -> tNode.isCommitted() && tNode.getOriginNode().getId() == nodeId)
                .map(tNode -> tNode.getOriginNode()).findAny();
    }


    //      TGraph IMPLEMENTATION
    class TGraphImpl implements TGraph {
        private IdGenerator idGenerator = new IdGenerator();
        private Set<TNode> tNodes = new HashSet<>();

        @Override
        public Node createNode(Label label, Properties properties) {
            Node node = new NodeImpl(idGenerator.generateId());
            if (label != null)
                node.setLabel(label);

            if (properties != null)
                node.setProperties(properties);

            TNode tNode = new TNode(transaction.getId(), node);
            tNodes.add(tNode);
            return node;
        }

        @Override
        public Node setProperty(long nodeId, Property property) {
            Optional<TNode> tNode = getTNodeById(nodeId);
            if (tNode.isPresent()) {
                tNode.get().setProperty(transaction.getId(), property);
                return tNode.get().getWorkingNode(transaction.getId());
            }
            else
                throw new NotFoundException("");
        }

        @Override
        public Optional<Node> getNodeById(long nodeId) {
            return Optional.of(getTNodeById(nodeId).get().getWorkingNode(transaction.getId()));
        }

        @Override
        public Set<Node> getAllNodes() {
            return tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()))
                    .map(tNode -> tNode.getWorkingNode(transaction.getId()))
                    .collect(Collectors.toSet());
        }

        @Override
        public Node deleteNode(long nodeId) {
            Optional<TNode> tNode = getTNodeById(nodeId);
            if (tNode.isPresent()) {
                tNode.get().deleteNode(transaction.getId());
                return tNode.get().getOriginNode();
            }

            throw new NotFoundException("Node does not exist!");
        }

        @Override
        public void commit() {
            if (transaction.isPerformed())
                throw new TransactionException("Transaction was performed!");

            // delete nodes if needed
            Set<TNode> nodesReadyToDelete = tNodes.stream().filter(TNode::isDeleted).collect(Collectors.toSet());
            tNodes.removeAll(nodesReadyToDelete);

            Set<TNode> nodesReadyToCommit = tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()) == true)
                    .collect(Collectors.toSet());

            nodesReadyToCommit.forEach(tNode -> tNode.commitTransaction(transaction.getId()));
            transaction.commit();
        }

        @Override
        public void rollback() {
            tNodes.forEach(tNode -> tNode.rollbackTransaction(transaction.getId()));
            transaction.rollback();
        }

        private Optional<TNode> getTNodeById(long id) {
            return tNodes.stream()
                    .filter(tNode -> tNode.isCommitted() == true && tNode.getOriginNode().getId() == id)
                    .findFirst();
        }
    }
}
