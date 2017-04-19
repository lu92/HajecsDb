package org.hajecsdb.graphs.storage.serializers;

import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.NotFoundException;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.storage.ByteUtils;
import org.hajecsdb.graphs.storage.entities.*;
import org.hajecsdb.graphs.storage.mappers.PropertiesBinaryMapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class EntitySerializer<EntityObject extends Entity> {
    private String filename;
    private String metadataFilename;
    private PropertiesBinaryMapper propertiesBinaryMapper;

    EntitySerializer(String filename, String entityMetadataFilename) {
        this.filename = filename;
        this.metadataFilename = entityMetadataFilename;
        this.propertiesBinaryMapper = new PropertiesBinaryMapper();
    }

    abstract EntityObject buildEntityType(long id, Properties properties);

    public BinaryEntity save(EntityObject entity) throws IOException {

        RandomAccessFile entitiesAccessFile = new RandomAccessFile(filename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(metadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0));
            metaDataAccessFile.seek(0);
        }

        BinaryProperties binaryProperties = propertiesBinaryMapper.toBinaryFigure(entity.getAllProperties());
        BinaryEntity binaryEntity = new BinaryEntity(entity.getId(), binaryProperties);

        // increment number of entities
        long numberOfEntities = metaDataAccessFile.readLong();
        numberOfEntities++;
        metaDataAccessFile.seek(0);
        metaDataAccessFile.writeLong(numberOfEntities);

        // save binary entity
        entitiesAccessFile.seek(entitiesAccessFile.length());
        long beforeSaveContentSize = entitiesAccessFile.length();
        entitiesAccessFile.write(binaryEntity.getBytes());
        long afterSaveContentSize = entitiesAccessFile.length();


        // create metadata
        MetaData metaData = new MetaData(entity.getId(), beforeSaveContentSize, afterSaveContentSize);

        // save metadata
        metaDataAccessFile.seek(metaDataAccessFile.length());
        metaDataAccessFile.write(metaData.getBytes());

        // close files
        entitiesAccessFile.close();
        metaDataAccessFile.close();

        return binaryEntity;
    }

    public Optional<EntityObject> read(long id) throws IOException {
        RandomAccessFile entitiesAccessFile = new RandomAccessFile(filename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(metadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0));
            metaDataAccessFile.seek(0);
        }

        // read number of entities
        metaDataAccessFile.seek(0);
        long numberOfEntities = metaDataAccessFile.readLong();

        if (numberOfEntities == 0) {
            return Optional.empty();
        }

        // get metaData
        for (int i = 0; i < numberOfEntities;) {
            long entityId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                if (entityId == id) {
                    int entitySectionLength = (int) (endDataSection - beginDataSection);
                    byte[] byteArray = new byte[entitySectionLength];
                    entitiesAccessFile.seek(beginDataSection);
                    entitiesAccessFile.readFully(byteArray, 0, entitySectionLength);
                    Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
                    EntityObject entity = buildEntityType(entityId, properties);
                    return Optional.of(entity);
                }
                i++;    // increment only when entity exist!
            }
        }

        // close files
        entitiesAccessFile.close();
        metaDataAccessFile.close();

        return Optional.empty();
    }

    public List<EntityObject> readAll() throws IOException {
        RandomAccessFile entitiesAccessFile = new RandomAccessFile(filename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(metadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of entities
        metaDataAccessFile.seek(0);
        long numberOfEntities = metaDataAccessFile.readLong();

        // get metaData
        MetaDataList metaDataList = new MetaDataList(numberOfEntities);
        for (int i = 0; i < numberOfEntities;) {
            long entityId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            long beginDataSection = metaDataAccessFile.readLong();
            long endDataSection = metaDataAccessFile.readLong();
            if (!deleted) {
                MetaData metaData = new MetaData(entityId, beginDataSection, endDataSection);
                metaDataList.add(metaData);
                i++;    // increment only when entity exist!
            }
        }

        List<EntityObject> entityList = new ArrayList<>((int) numberOfEntities);

        // get Entities
        for (MetaData metaData : metaDataList.getMetaDataList()) {
            int entitySectionLength = (int) (metaData.getEndDataSection() - metaData.getBeginDataSection());
            byte[] byteArray = new byte[entitySectionLength];
            entitiesAccessFile.seek(metaData.getBeginDataSection());
            entitiesAccessFile.readFully(byteArray, 0, entitySectionLength);
            long entityId = ByteUtils.bytesToLong(Arrays.copyOfRange(byteArray, 0, Long.BYTES));
            Properties properties = propertiesBinaryMapper.toProperties(Arrays.copyOfRange(byteArray, Long.BYTES, byteArray.length));
            EntityObject entity = buildEntityType(entityId, properties);
            entityList.add(entity);
        }

        // close files
        entitiesAccessFile.close();
        metaDataAccessFile.close();

        return entityList;
    }

    public BinaryEntity update(EntityObject entity) throws IOException, NotFoundException {
        delete(entity.getId());
        return save(entity);
    }

    public void delete(long id) throws IOException, NotFoundException {
        if (id <= 0) {
            throw new NotFoundException("Not found entity with id: " + id);
        }

        RandomAccessFile entitiesAccessFile = new RandomAccessFile(filename, "rw");
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(metadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of entities
        metaDataAccessFile.seek(0);
        long numberOfEntities = metaDataAccessFile.readLong();

        if (numberOfEntities == 0) {
            throw new NotFoundException("Not found entity with id: " + id);
        }

        // get metaData
        for (int i = 0; i < numberOfEntities; i++) {
            long entityId = metaDataAccessFile.readLong();
            boolean deleted = metaDataAccessFile.readByte() == 0;
            metaDataAccessFile.readLong();
            metaDataAccessFile.readLong();

            if (!deleted && entityId == id) {

                // delete entity from metadata
                long position = metaDataAccessFile.getFilePointer() - 2 * Long.BYTES - 1;
                metaDataAccessFile.seek(position);
                metaDataAccessFile.writeByte(0); // mark entity as deleted

                // decrease number of entity
                metaDataAccessFile.seek(0);
                metaDataAccessFile.writeLong(--numberOfEntities);

                return;
            }
        }

        // close files
        entitiesAccessFile.close();
        metaDataAccessFile.close();

        throw new NotFoundException("Not found entity with id: " + id);
    }

    public long count() throws IOException {
        RandomAccessFile metaDataAccessFile = new RandomAccessFile(metadataFilename, "rw");

        if (metaDataAccessFile.length() == 0) {
            // insert 0 if file is empty
            metaDataAccessFile.write(ByteUtils.longToBytes(0l));
            metaDataAccessFile.seek(0);
        }

        // read number of entities
        metaDataAccessFile.seek(0);
        long numberOfEntities = metaDataAccessFile.readLong();
        metaDataAccessFile.close();
        return numberOfEntities;
    }
}