package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Relationship;
import org.hajecsdb.graphs.core.impl.RelationshipImpl;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.entities.BinaryProperties;
import org.hajecsdb.graphs.storage.entities.BinaryRelationship;
import org.hajecsdb.graphs.storage.entities.RelationshipMetaData;
import org.hajecsdb.graphs.storage.entities.RelationshipMetaDataList;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RelationshipSerializer implements Serializer<Relationship, BinaryRelationship> {
    private String relationshipsFilename;
    private String relationshipMetadataFilename;
    private PropertiesBinaryMapper propertiesBinaryMapper;

    public RelationshipSerializer(String relationshipsFilename, String relationshipMetadataFilename) {
        this.relationshipsFilename = relationshipsFilename;
        this.relationshipMetadataFilename = relationshipMetadataFilename;
        this.propertiesBinaryMapper = new PropertiesBinaryMapper();
    }

    @Override
    public BinaryRelationship save(Relationship relationship) throws IOException {
        RandomAccessFile relationshipAccessFile = new RandomAccessFile(relationshipsFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(relationshipMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(relationship.getAllProperties());
        BinaryRelationship binaryRelationship = new BinaryRelationship(relationship.getId(), binaryProperties);

        // increment number of nodes
        long numberOfNodes = metaDataAccessFile.readLong();
        numberOfNodes++;
        metaDataAccessFile.seek(0);
        metaDataAccessFile.writeLong(numberOfNodes);

        // save binary node
        relationshipAccessFile.seek(relationshipAccessFile.length());
        long beforeSaveNodeContentSize = relationshipAccessFile.length();
        relationshipAccessFile.write(binaryRelationship.getBytes());
        long afterSaveNodeContentSize = relationshipAccessFile.length();


        // create metadata
        RelationshipMetaData relationshipMetaData = new RelationshipMetaData(relationship.getId(), beforeSaveNodeContentSize, afterSaveNodeContentSize);

        // save metadata
        metaDataAccessFile.seek(metaDataAccessFile.length());
        metaDataAccessFile.write(relationshipMetaData.getBytes());

        // close files
        relationshipAccessFile.close();
        metaDataAccessFile.close();

        return binaryRelationship;
    }

    @Override
    public Optional<Relationship> read(long id) throws IOException {
        RandomAccessFile relationshipsAccessFile = new RandomAccessFile(relationshipsFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(relationshipMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfRelationships = metaDataAccessFile.readLong();

        if (numberOfRelationships == 0) {
            return Optional.empty();
        }

        // get metaData
        for (int i = 0; i < numberOfRelationships;) {
            long relationshipId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                if (relationshipId == id) {
                    int relationshipSectionLength = (int) (endDataSection - beginDataSection);
                    byte[] byteArray = new byte[relationshipSectionLength];
                    relationshipsAccessFile.seek(beginDataSection);
                    relationshipsAccessFile.readFully(byteArray, 0, relationshipSectionLength);
                    Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
                    RelationshipImpl relationship = new RelationshipImpl(relationshipId);
                    relationship.deleteProperties("id");
                    relationship.setProperties(properties);
                    return Optional.of(relationship);
                }
                i++;    // increment only when node exist!
            }
        }

        // close files
        relationshipsAccessFile.close();
        metaDataAccessFile.close();

        return Optional.empty();    }

    @Override
    public List<Relationship> readAll() throws IOException {
        RandomAccessFile relationshipsAccessFile = new RandomAccessFile(relationshipsFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(relationshipMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfRelationships = metaDataAccessFile.readLong();

        // get metaData
        RelationshipMetaDataList relationshipMetaDataList = new RelationshipMetaDataList(numberOfRelationships);
        for (int i = 0; i < numberOfRelationships;) {
            long relationshipId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                RelationshipMetaData nodeMetaData = new RelationshipMetaData(relationshipId, beginDataSection, endDataSection);
                relationshipMetaDataList.add(nodeMetaData);
                i++;    // increment only when node exist!
            }
        }

        List<Relationship> nodeList = new ArrayList<>((int) numberOfRelationships);

        // get Nodes
        for (RelationshipMetaData metaData : relationshipMetaDataList.getMetaDataList()) {
            int nodeSectionLength = (int) (metaData.getEndDataSection() - metaData.getBeginDataSection());
            byte[] byteArray = new byte[nodeSectionLength];
            relationshipsAccessFile.seek(metaData.getBeginDataSection());
            relationshipsAccessFile.readFully(byteArray, 0, nodeSectionLength);
            long relationshipId = ByteUtils.bytesToLong(Arrays.copyOfRange(byteArray, 0, Long.BYTES));
            Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
            Relationship relationship = new RelationshipImpl(relationshipId);
            relationship.setProperties(properties);
            nodeList.add(relationship);
        }

        // close files
        relationshipsAccessFile.close();
        metaDataAccessFile.close();

        return nodeList;
    }

    @Override
    public BinaryRelationship update(Relationship relationship) throws IOException, NotFoundException {
        delete(relationship.getId());
        return save(relationship);
    }

    @Override
    public void delete(long id) throws IOException, NotFoundException {
        if (id <= 0) {
            throw new NotFoundException("Not found relationship with nodeId: " + id);
        }

        RandomAccessFile relationshipsAccessFile = new RandomAccessFile(relationshipsFilename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(relationshipMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfRelationships = metaDataAccessFile.readLong();

        if (numberOfRelationships == 0) {
            throw new NotFoundException("Not found relationship with nodeId: " + id);
        }

        // get metaData
        for (int i = 0; i < numberOfRelationships; i++) {
            long relationshipId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            metaDataAccessFile.readLong();
            metaDataAccessFile.readLong();

            if (!deleted && relationshipId == id) {

                // delete node in metadata
                long position = metaDataAccessFile.getFilePointer() - 2 * Long.BYTES - 1;
                metaDataAccessFile.seek(position);
                metaDataAccessFile.writeByte(0); // mark node as deleted

                // decrease number of nodes
                metaDataAccessFile.seek(0);
                metaDataAccessFile.writeLong(--numberOfRelationships);

                return;
            }
        }

        // close files
        relationshipsAccessFile.close();
        metaDataAccessFile.close();

        throw new NotFoundException("Not found relationship with nodeId: " + id);
    }

    @Override
    public long count() throws IOException {
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(relationshipMetadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 number of nodes if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of nodes
        metaDataAccessFile.seek(0);
        long numberOfRelationships = metaDataAccessFile.readLong();
        metaDataAccessFile.close();
        return numberOfRelationships;
    }
}
