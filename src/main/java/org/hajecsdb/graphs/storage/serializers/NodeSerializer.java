package org.hajecsdb.graphs.storage.serializers;


import org.hajecsdb.graphs.core.Node;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.impl.NodeImpl;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.entities.BinaryNode;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.NodeMetaData;
import org.hajecsdb.graphs.storage.entities.NodeMetaDataList;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class NodeSerializer implements Serializer<Node, BinaryNode> {
    private String nodesFilename;
    private String nodeMetadataFilename;
    private PropertiesBinaryMapper propertiesBinaryMapper;

    public NodeSerializer(String nodesFilename, String nodeMetadataFilename) {
        this.nodesFilename = nodesFilename;
        this.nodeMetadataFilename = nodeMetadataFilename;
        this.propertiesBinaryMapper = new PropertiesBinaryMapper();
    }

    @Override
    public BinaryNode save(Node node) throws IOException {

        RandomAccessFile nodesAccessFile = new RandomAccessFile(nodesFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(nodeMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(node.getAllProperties());
        BinaryNode binaryNode = new BinaryNode(node.getId(), binaryProperties);

        // increment number of nodes
        long numberOfNodes = metaDataAccessFile.readLong();
        numberOfNodes++;
        metaDataAccessFile.seek(0);
        metaDataAccessFile.writeLong(numberOfNodes);

        // save binary node
        nodesAccessFile.seek(nodesAccessFile.length());
        long beforeSaveNodeContentSize = nodesAccessFile.length();
        nodesAccessFile.write(binaryNode.getBytes());
        long afterSaveNodeContentSize = nodesAccessFile.length();


        // create metadata
        NodeMetaData nodeMetaData = new NodeMetaData(node.getId(), beforeSaveNodeContentSize, afterSaveNodeContentSize);

        // save metadata
        metaDataAccessFile.seek(metaDataAccessFile.length());
        metaDataAccessFile.write(nodeMetaData.getBytes());

        // close files
        nodesAccessFile.close();
        metaDataAccessFile.close();

        return binaryNode;
    }

    @Override
    public Optional<Node> read(long id) throws IOException {
        RandomAccessFile nodesAccessFile = new RandomAccessFile(nodesFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(nodeMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfNodes = metaDataAccessFile.readLong();

        if (numberOfNodes == 0) {
            return Optional.empty();
        }

        // get metaData
        for (int i = 0; i < numberOfNodes;) {
            long nodeId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                if (nodeId == id) {
                    int nodeSectionLength = (int) (endDataSection - beginDataSection);
                    byte[] byteArray = new byte[nodeSectionLength];
                    nodesAccessFile.seek(beginDataSection);
                    nodesAccessFile.readFully(byteArray, 0, nodeSectionLength);
                    Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
                    Node node = new NodeImpl(nodeId);
                    node.setProperties(properties);
                    return Optional.of(node);
                }
                i++;    // increment only when node exist!
            }
        }

        // close files
        nodesAccessFile.close();
        metaDataAccessFile.close();

        return Optional.empty();
    }

    @Override
    public List<Node> readAll() throws IOException {
        RandomAccessFile nodesAccessFile = new RandomAccessFile(nodesFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(nodeMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfNodes = metaDataAccessFile.readLong();

        // get metaData
        NodeMetaDataList nodeMetaDataList = new NodeMetaDataList(numberOfNodes);
        for (int i = 0; i < numberOfNodes;) {
            long nodeId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                NodeMetaData nodeMetaData = new NodeMetaData(nodeId, beginDataSection, endDataSection);
                nodeMetaDataList.add(nodeMetaData);
                i++;    // increment only when node exist!
            }
        }

        List<Node> nodeList = new ArrayList<>((int) numberOfNodes);

        // get Nodes
        for (NodeMetaData metaData : nodeMetaDataList.getNodeMetaDataList()) {
            int nodeSectionLength = (int) (metaData.getEndDataSection() - metaData.getBeginDataSection());
            byte[] byteArray = new byte[nodeSectionLength];
            nodesAccessFile.seek(metaData.getBeginDataSection());
            nodesAccessFile.readFully(byteArray, 0, nodeSectionLength);
            long nodeId = ByteUtils.bytesToLong(Arrays.copyOfRange(byteArray, 0, Long.BYTES));
            Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
            Node node = new NodeImpl(nodeId);
            node.setProperties(properties);
            nodeList.add(node);
        }

        // close files
        nodesAccessFile.close();
        metaDataAccessFile.close();

        return nodeList;
    }

    @Override
    public BinaryNode update(Node node) throws IOException, NodeNotFoundException {
        delete(node.getId());
        return save(node);
    }

    @Override
    public void delete(long id) throws IOException, NodeNotFoundException {
        if (id <= 0) {
            throw new NodeNotFoundException(id);
        }

        RandomAccessFile nodesAccessFile = new RandomAccessFile(nodesFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(nodeMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfNodes = metaDataAccessFile.readLong();

        if (numberOfNodes == 0) {
            throw new NodeNotFoundException(id);
        }

        // get metaData
        for (int i = 0; i < numberOfNodes; i++) {
            long nodeId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            metaDataAccessFile.readLong();
            metaDataAccessFile.readLong();

            if (!deleted && nodeId == id) {

                // delete node in metadata
                long position = metaDataAccessFile.getFilePointer() - 2 * Long.BYTES - 1;
                metaDataAccessFile.seek(position);
                metaDataAccessFile.writeByte(0); // mark node as deleted

                // decrease number of nodes
                metaDataAccessFile.seek(0);
                metaDataAccessFile.writeLong(--numberOfNodes);

                return;
            }
        }

        // close files
        nodesAccessFile.close();
        metaDataAccessFile.close();

        throw new NodeNotFoundException(id);
    }

    @Override
    public long count() throws IOException {
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(nodeMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfNodes = metaDataAccessFile.readLong();
        metaDataAccessFile.close();
        return numberOfNodes;
    }
}
