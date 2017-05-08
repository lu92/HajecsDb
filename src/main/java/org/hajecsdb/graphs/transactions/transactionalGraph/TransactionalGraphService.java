package org.hajecsdb.graphs.transactions.transactionalGraph;

import org.hajecsdb.graphs.IdGenerator;
import org.hajecsdb.graphs.core.*;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.impl.NodeImpl;
import org.hajecsdb.graphs.transactions.Transaction;
import org.hajecsdb.graphs.transactions.exceptions.TransactionException;

import java.util.*;
import java.util.stream.Collectors;

public class TransactionalGraphService {

    private Transaction transaction;
    private TGraphImpl tGraph = new TGraphImpl();
    private Set<Transaction> supportedTransactions = new HashSet<>();


    public TGraph context(Transaction transaction) throws TransactionException {
        if (transaction == null)
            throw new TransactionException("Not defined transaction!");

        if (transaction.isPerformed())
            throw new TransactionException("Transaction was performed! [COMMITED OR ROLLBACKED]");

        if (!supportedTransactions.contains(transaction)) {
            createDedicatedTransactionWorkForEachPersistentEntity(transaction.getId());
            supportedTransactions.add(transaction);
        }

        this.transaction = transaction;
        return tGraph;
    }

    private synchronized void createDedicatedTransactionWorkForEachPersistentEntity(long transactionId) {
        tGraph.tNodes.stream()
                .filter(tNode -> getAllPersistentNodes().contains(tNode.getOriginNode()))
                .forEach(tNode -> tNode.createTransactionWork(transactionId));
        tGraph.tRelationships.stream()
                .filter(tRelationship -> getAllPersistentNodes().contains(tRelationship.getOriginRelationship()))
                .forEach(tRelationship -> tRelationship.createTransactionWork(transactionId));
    }

    public Set<Node> getAllPersistentNodes() {
        return tGraph.tNodes.stream().filter(AbstractTransactionalEntity::isCommitted).map(TNode::getOriginNode).collect(Collectors.toSet());
    }

    public Set<Relationship> getAllPersistentRelationships() {
        return tGraph.tRelationships.stream().filter(AbstractTransactionalEntity::isCommitted).map(TRelationship::getOriginRelationship).collect(Collectors.toSet());
    }

    public Optional<Node> getPersistentNodeById(long nodeId) {
        return tGraph.tNodes.stream()
                .filter(tNode -> tNode.isCommitted() && tNode.getOriginNode().getId() == nodeId)
                .map(TNode::getOriginNode).findFirst();
    }

    public Optional<Relationship> getPersistentRelationshipById(long relationshipId) {
        return tGraph.tRelationships.stream()
                .filter(tRelationship -> tRelationship.isCommitted() && tRelationship.getOriginRelationship().getId() == relationshipId)
                .map(TRelationship::getOriginRelationship).findFirst();
    }


    //      TGraph IMPLEMENTATION
    private class TGraphImpl implements TGraph {
        private IdGenerator idGenerator = new IdGenerator();
        private Set<TNode> tNodes = new HashSet<>();
        private Set<TRelationship> tRelationships = new HashSet<>();

        @Override
        public Node createNode(Label label, Properties properties) {
            Node node = new NodeImpl(idGenerator.generateId());
            if (label != null)
                node.setLabel(label);

            if (properties != null)
                node.setProperties(properties);

            TNode tNode = new TNode(node);
            tNode.createTransactionWork(transaction.getId());

            tNodes.add(tNode);
            return node;
        }

        @Override
        public Optional<Node> getNodeById(long nodeId) {
            Optional<TNode> tNode = getTNodeById(transaction.getId(), nodeId);
            if (!tNode.isPresent() || tNode.get().isDeleted(transaction.getId()))
                return Optional.empty();

            return Optional.of((Node) getTNodeById(transaction.getId(), nodeId).get().getWorkingEntity(transaction.getId()));
        }

        @Override
        public Set<Node> getAllNodes() {
            return tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()))
                    .map(tNode -> (Node) tNode.getWorkingEntity(transaction.getId()))
                    .collect(Collectors.toSet());
        }

        @Override
        public Node deleteNode(long nodeId) {
            Optional<TNode> tNode = getTNodeById(transaction.getId(), nodeId);
            if (tNode.isPresent()) {
                tNode.get().delete(transaction.getId());
                return tNode.get().getOriginNode();
            }

            throw new NotFoundException("Node does not exist!");
        }

        @Override
        public Node setPropertyToNode(long nodeId, Property property) {
            Optional<TNode> tNode = getTNodeById(transaction.getId(), nodeId);
            if (tNode.isPresent()) {
                tNode.get().setProperty(transaction.getId(), property);
                return (Node) tNode.get().getWorkingEntity(transaction.getId());
            }
            else
                throw new NotFoundException("");
        }

        @Override
        public void deletePropertyFromNode(long nodeId, String propertyKey) {
            Optional<TNode> tNode = getTNodeById(transaction.getId(), nodeId);
            if (tNode.isPresent()) {
                tNode.get().deleteProperty(transaction.getId(), propertyKey);
            } else
                throw new NotFoundException("Node does not exist!");
        }

        @Override
        public Relationship createRelationship(long startNodeId, long endNodeId, Label label) {

            Optional<TNode> startTNode = getTNodeById(transaction.getId(), startNodeId);
            Optional<TNode> endTNode = getTNodeById(transaction.getId(), endNodeId);

            if (!(startTNode.isPresent() && endTNode.isPresent()))
                throw new NotFoundException("Cannot found one or both nodes!");

            if (label == null)
                throw new NullPointerException("Relationship must have label!");

            if (!startTNode.get().containsTransactionChanges(transaction.getId())) {
                startTNode.get().createTransactionWork(transaction.getId());
            }

            if (!endTNode.get().containsTransactionChanges(transaction.getId())) {
                endTNode.get().createTransactionWork(transaction.getId());
            }

            TRelationship tRelationship = new TRelationship(
                    transaction.getId(), idGenerator.generateId(), startTNode.get(), endTNode.get(), label);

            tRelationship.createTransactionWork(transaction.getId());

            tRelationships.add(tRelationship);
            return tRelationship.getOriginRelationship();
        }

        @Override
        public Relationship deleteRelationship(long relationshipId) {
            Optional<TRelationship> tRelationship = getTRelationshipById(relationshipId);
            if (tRelationship.isPresent()) {

//                tRelationship.get().delete(relationshipId);
//                return tRelationship.get().getOriginRelationship();

                if (!tRelationship.get().isTransactionWorkExists(transaction.getId())) {
                    tRelationship.get().createTransactionWork(transaction.getId());
                }

                Relationship deletedRelationship = (Relationship) tRelationship.get().getWorkingEntity(transaction.getId());
                tRelationship.get().delete(transaction.getId());
                ((Node) getTNodeById(transaction.getId(), deletedRelationship.getStartNode().getId()).get().getWorkingEntity(transaction.getId())).getRelationships().remove(deletedRelationship);
                ((Node) getTNodeById(transaction.getId(), deletedRelationship.getEndNode().getId()).get().getWorkingEntity(transaction.getId())).getRelationships().remove(deletedRelationship);
                return deletedRelationship;
            }

            throw new NotFoundException("Relationship does not exist!");
        }

        @Override
        public Relationship setPropertyToRelationship(long relationshipId, Property property) {
            Optional<TRelationship> tRelationship = getTRelationshipById(relationshipId);
            if (tRelationship.isPresent()) {
                tRelationship.get().setProperty(transaction.getId(), property);
                return (Relationship) tRelationship.get().getWorkingEntity(transaction.getId());
            }
            else
                throw new NotFoundException("");
        }

        @Override
        public void deletePropertyFromRelationship(int relationshipId, String propertyKey) {
            Optional<TRelationship> tRelationship = getTRelationshipById(relationshipId);
            if (tRelationship.isPresent()) {
                tRelationship.get().deleteProperty(transaction.getId(), propertyKey);
            } else
                throw new NotFoundException("Relationship does not exist!");
        }

        @Override
        public void commit() {
            if (transaction.isPerformed())
                throw new TransactionException("Transaction was performed!");

            // delete nodes if needed
            Set<TNode> nodesReadyToDelete = tNodes.stream()
                    .filter(tNode -> tNode.isDeleted(transaction.getId()))
                    .collect(Collectors.toSet());
            tNodes.removeAll(nodesReadyToDelete);

            // COMMIT NODES
            Set<TNode> nodesReadyToCommit = tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()))
                    .collect(Collectors.toSet());

            nodesReadyToCommit.forEach(tNode -> tNode.commitTransaction(transaction.getId()));

            // delete relationships if needed
            Set<TRelationship> relationshipsReadyToDelete = tRelationships.stream()
                    .filter(tRelationship -> tRelationship.isDeleted(transaction.getId()))
                    .collect(Collectors.toSet());
            tRelationships.removeAll(relationshipsReadyToDelete);

            // COMMIT RELATIONSHIPS
            Set<TRelationship> relationshipsReadyToCommit = tRelationships.stream()
                    .filter(tRelationship -> tRelationship.containsTransactionChanges(transaction.getId()))
                    .collect(Collectors.toSet());
            relationshipsReadyToCommit.forEach(tRelationship -> tRelationship.commitTransaction(transaction.getId()));

            transaction.commit();
            supportedTransactions.remove(transaction);
        }

        @Override
        public void rollback() {
            tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()))
                    .forEach(tNode -> tNode.rollbackTransaction(transaction.getId()));
            transaction.rollback();
            supportedTransactions.remove(transaction);
        }

        private Optional<TNode> getTNodeById(long transactionId, long nodeId) {
            return tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transactionId) && tNode.getOriginNode().getId() == nodeId)
                    .findFirst();
        }

        private Optional<TRelationship> getTRelationshipById(long id) {
            return tRelationships.stream()
                    .filter(tRelationship -> tRelationship.getOriginRelationship().getId() == id)
                    .findFirst();
        }
    }
}
