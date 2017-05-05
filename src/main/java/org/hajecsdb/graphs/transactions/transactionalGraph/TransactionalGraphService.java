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

    public Optional<Relationship> getPersistentRelationshipById(long relationshipId) {
        return tGraph.tRelationships.stream()
                .filter(tRelationship -> tRelationship.isCommitted() && tRelationship.originRelationship.getId() == relationshipId)
                .map(tRelationship -> tRelationship.originRelationship).findAny();
    }


    //      TGraph IMPLEMENTATION
    class TGraphImpl implements TGraph {
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

            TNode tNode = new TNode(transaction.getId(), node);
            tNodes.add(tNode);
            return node;
        }

        @Override
        public Node setPropertyToNode(long nodeId, Property property) {
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
        public void deletePropertyFromNode(int nodeId, String propertyKey) {
            Optional<TNode> tNode = getTNodeById(nodeId);
            if (tNode.isPresent()) {
                tNode.get().deleteProperty(transaction.getId(), propertyKey);
            } else
                throw new NotFoundException("Relationship does not exist!");
        }

        @Override
        public Relationship createRelationship(long startNodeId, long endNodeId, Label label) {
            Optional<TNode> startTNode = getTNodeById(startNodeId);
            Optional<TNode> endTNode = getTNodeById(endNodeId);

            if (!(startTNode.isPresent() && endTNode.isPresent()))
                throw new NotFoundException("Cannot found one or both nodes!");

            if (label == null)
                throw new IllegalArgumentException("Relationship must have label!");

            if (!startTNode.get().containsTransactionChanges(transaction.getId())) {
                startTNode.get().createTransactionWork(transaction.getId());
            }

            if (!endTNode.get().containsTransactionChanges(transaction.getId())) {
                endTNode.get().createTransactionWork(transaction.getId());
            }

            TRelationship tRelationship = new TRelationship(
                    transaction.getId(), idGenerator.generateId(), startTNode.get(), endTNode.get(), label);

            tRelationships.add(tRelationship);
            return tRelationship.originRelationship;
        }

        @Override
        public Relationship setPropertyToRelationship(long relationshipId, Property property) {
            Optional<TRelationship> tRelationship = getTRelationshipById(relationshipId);
            if (tRelationship.isPresent()) {
                tRelationship.get().setProperty(transaction.getId(), property);
                return tRelationship.get().getWorkingRelationship(transaction.getId());
            }
            else
                throw new NotFoundException("");
        }

        @Override
        public Relationship deleteRelationship(long id) {
            Optional<TRelationship> tRelationship = getTRelationshipById(id);
            if (tRelationship.isPresent()) {
                Relationship deletedRelationship = tRelationship.get().deleteRelationship(transaction.getId());
                getTNodeById(deletedRelationship.getStartNode().getId()).get().getWorkingNode(transaction.getId()).getRelationships().remove(deletedRelationship);
                getTNodeById(deletedRelationship.getEndNode().getId()).get().getWorkingNode(transaction.getId()).getRelationships().remove(deletedRelationship);
                return deletedRelationship;
            }

            throw new NotFoundException("Relationship does not exist!");
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
            Set<TNode> nodesReadyToDelete = tNodes.stream().filter(TNode::isDeleted).collect(Collectors.toSet());
            tNodes.removeAll(nodesReadyToDelete);

            // COMMIT NODES
            Set<TNode> nodesReadyToCommit = tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()) == true)
                    .collect(Collectors.toSet());

            nodesReadyToCommit.forEach(tNode -> tNode.commitTransaction(transaction.getId()));

            // delete relationships if needed
            Set<TRelationship> relationshipsReadyToDelete = tRelationships.stream().filter(TRelationship::isDeleted).collect(Collectors.toSet());
            tRelationships.removeAll(relationshipsReadyToDelete);

            // COMMIT RELATIONSHIPS
            Set<TRelationship> relationshipsReadyToCommit = tRelationships.stream()
                    .filter(tRelationship -> tRelationship.containsTransactionChanges(transaction.getId()) == true)
                    .collect(Collectors.toSet());
            relationshipsReadyToCommit.forEach(tRelationship -> tRelationship.commitTransaction(transaction.getId()));

            transaction.commit();
        }

        @Override
        public void rollback() {
            tNodes.stream()
                    .filter(tNode -> tNode.containsTransactionChanges(transaction.getId()))
                    .forEach(tNode -> tNode.rollbackTransaction(transaction.getId()));
            transaction.rollback();
        }

        private Optional<TNode> getTNodeById(long nodeId) {
            return tNodes.stream()
                    .filter(tNode -> tNode.getOriginNode().getId() == nodeId)
                    .findFirst();
        }

        private Optional<TRelationship> getTRelationshipById(long id) {
            return tRelationships.stream()
                    .filter(tRelationship -> tRelationship.getOriginRelationship().getId() == id)
                    .findFirst();
        }
    }
}
