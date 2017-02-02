package org.hajecsdb.graphs.storage.serializers;


import org.hajecsdb.graphs.core.Entity;
import org.hajecsdb.graphs.core.Properties;
import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.storage.EntityType;
import org.hajecsdb.graphs.storage.entities.BinaryEntity;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class EntitySerializer {

    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    public BinaryEntity serializeEntity(Path targetPath, Path targetMetaDataPath, Entity entity, EntityType type) throws IOException {
        SeekableByteChannel nodesChannel = Files.newByteChannel(targetPath, StandardOpenOption.APPEND);

        List<BinaryProperty> binaryProperties = new ArrayList<>();
        for (Property property : entity.getAllProperties().getAllProperties()) {
            BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
            binaryProperties.add(binaryProperty);
        }
        BinaryEntity binaryEntity = new BinaryEntity(nodesChannel.position(), binaryProperties);
        nodesChannel.write(ByteBuffer.wrap(binaryEntity.getBytes()));
        nodesChannel.close();

//        if (type == EntityType.NODE) {
//            SeekableByteChannel nodesMetaDataChannel = Files.newByteChannel(targetMetaDataPath, StandardOpenOption.APPEND);
//            BinaryNode binaryNode =
//                    new BinaryNode(entity.getId(), nodesMetaDataChannel.position(), binaryProperties);
//
//
//            byteHelper.
//
//        }


        return binaryEntity;
    }

    public Properties deserializeEntitiesProperties(Path path, long id, EntityType type) throws IOException {
        SeekableByteChannel seekableByteChannel = Files.newByteChannel(path, StandardOpenOption.READ);
        Properties properties = new Properties();
        ByteBuffer bf = ByteBuffer.allocate(24);
        while((seekableByteChannel.read(bf))>0){
            bf.flip();
            Property deserializedProperty = propertiesBinaryMapper.toProperty(bf.array());
            properties.add(deserializedProperty);
            System.out.println(deserializedProperty);
            bf.clear();
        }
        return properties;
    }
}
