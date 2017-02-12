package HajecsDb.graphSerialization;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.hajecsdb.graphs.storage.entities.PropertyHeader;
import org.hajecsdb.graphs.storage.entities.PropertyHeaderMapper;
import org.hajecsdb.graphs.storage.serializers.PropertiesBinaryMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

import static org.hajecsdb.graphs.core.PropertyType.LONG;

@RunWith(MockitoJUnitRunner.class)
public class PropertyHeaderMapperTest {

    private PropertyHeaderMapper propertyHeaderMapper = new PropertyHeaderMapper();
    private PropertiesBinaryMapper propertiesBinaryMapper = new PropertiesBinaryMapper();

    @Test
    public void mapSingleProperty() {
        // given
        Property property = new Property("id", LONG, 1l);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);

        // when
        PropertyHeader propertyHeader = propertyHeaderMapper.mapPropertyHeader(binaryProperty);

        // then
        assertThat(propertyHeader.getLength()).isEqualTo(2*Long.BYTES+binaryProperty.getLength());

        long beginIndex = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeader.getBytes(), 0, Long.BYTES)).getLong();
        assertThat(beginIndex).isEqualTo(2*Long.BYTES);

        long endIndex = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeader.getBytes(), Long.BYTES, 2*Long.BYTES)).getLong();
        assertThat(endIndex).isEqualTo(2*Long.BYTES+binaryProperty.getLength());

        byte[] propertyBytes = Arrays.copyOfRange(propertyHeader.getBytes(), (int) beginIndex, (int) endIndex);
        Property decodedProperty = propertiesBinaryMapper.toProperty(propertyBytes);
        assertThat(decodedProperty.getKey()).isEqualTo("id");
        assertThat(decodedProperty.getType()).isEqualTo(LONG);
        assertThat(decodedProperty.getValue()).isEqualTo(1l);
    }

    @Test
    public void mapListWithOneProperty() {
        // given
        Property property = new Property("id", LONG, 1l);
        BinaryProperty binaryProperty = propertiesBinaryMapper.toBinaryFigure(property);
        List<BinaryProperty> binaryPropertyList = Arrays.asList(binaryProperty);

        // when
        List<PropertyHeader> propertyHeaders = propertyHeaderMapper.mapPropertyHeaders(binaryPropertyList);

        // then
        assertThat(binaryPropertyList).hasSize(1);
        assertThat(propertyHeaders.get(0).getLength()).isEqualTo(2*Long.BYTES+binaryProperty.getLength());

        long beginIndex = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), 0, Long.BYTES)).getLong();
        assertThat(beginIndex).isEqualTo(2*Long.BYTES);

        long endIndex = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), Long.BYTES, 2*Long.BYTES)).getLong();
        assertThat(endIndex).isEqualTo(2*Long.BYTES+binaryProperty.getLength());

        byte[] propertyBytes = Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), (int) beginIndex, (int) endIndex);
        Property decodedProperty = propertiesBinaryMapper.toProperty(propertyBytes);
        assertThat(decodedProperty.getKey()).isEqualTo("id");
        assertThat(decodedProperty.getType()).isEqualTo(LONG);
        assertThat(decodedProperty.getValue()).isEqualTo(1l);
    }

//    @Test
//    public void mapListWithThreeProperty() {
//        // given
//        Property property1 = new Property("id", LONG, 1l);
//        Property property2 = new Property("firstName", STRING, "Jan");
//        Property property3 = new Property("lastName", STRING, "Kowalski");
//        BinaryProperty expectedBinaryProperty1 = propertiesBinaryMapper.toBinaryFigure(property1);
//        BinaryProperty expectedBinaryProperty2 = propertiesBinaryMapper.toBinaryFigure(property2);
//        BinaryProperty expectedBinaryProperty3 = propertiesBinaryMapper.toBinaryFigure(property3);
//        List<BinaryProperty> binaryPropertyList = Arrays.asList(expectedBinaryProperty1, expectedBinaryProperty2, expectedBinaryProperty3);
//
//        // when
//        List<PropertyHeader> propertyHeaders = propertyHeaderMapper.mapPropertyHeaders(binaryPropertyList);
//
//        // then
//        assertThat(binaryPropertyList).hasSize(3);
//        assertThat(propertyHeaders.get(0).getLength()).isEqualTo(2*Long.BYTES+expectedBinaryProperty1.getLength());
//        assertThat(propertyHeaders.get(1).getLength()).isEqualTo(2*Long.BYTES+expectedBinaryProperty2.getLength());
//        assertThat(propertyHeaders.get(2).getLength()).isEqualTo(2*Long.BYTES+expectedBinaryProperty3.getLength());
//
//        long beginIndex1 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), 0, Long.BYTES)).getLong();
//        assertThat(beginIndex1).isEqualTo(2*Long.BYTES);
//
//        long endIndex1 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), Long.BYTES, 2*Long.BYTES)).getLong();
//        assertThat(endIndex1).isEqualTo(2*Long.BYTES+expectedBinaryProperty1.getLength());
//
//        byte[] propertyBytes1 = Arrays.copyOfRange(propertyHeaders.get(0).getBytes(), (int) beginIndex1, (int) endIndex1);
//        Property decodedProperty1 = propertiesBinaryMapper.toProperty(propertyBytes1);
//        assertThat(decodedProperty1.getKey()).isEqualTo("id");
//        assertThat(decodedProperty1.getType()).isEqualTo(LONG);
//        assertThat(decodedProperty1.getValue()).isEqualTo(1l);
//
//
//        long beginIndex2 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(1).getBytes(), 0, Long.BYTES)).getLong();
//        assertThat(beginIndex2).isEqualTo(endIndex1+2*Long.BYTES);
//
//        long endIndex2 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(1).getBytes(), Long.BYTES, 2*Long.BYTES)).getLong();
//        assertThat(endIndex2).isEqualTo(endIndex1+2*Long.BYTES+expectedBinaryProperty2.getLength());
//
//        byte[] propertyBytes2 = Arrays.copyOfRange(propertyHeaders.get(1).getBytes(), (int) beginIndex2, (int) endIndex2);
//        Property decodedProperty2 = propertiesBinaryMapper.toProperty(propertyBytes2);
//        assertThat(decodedProperty2.getKey()).isEqualTo("firstName");
//        assertThat(decodedProperty2.getType()).isEqualTo(STRING);
//        assertThat(decodedProperty2.getValue()).isEqualTo("Jan");
//
//        long beginIndex3 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(2).getBytes(), 0, Long.BYTES)).getLong();
//        assertThat(beginIndex3).isEqualTo(2*Long.BYTES);
//
//        long endIndex3 = ByteBuffer.wrap(Arrays.copyOfRange(propertyHeaders.get(2).getBytes(), Long.BYTES, 2*Long.BYTES)).getLong();
//        assertThat(endIndex3).isEqualTo(2*Long.BYTES+expectedBinaryProperty3.getLength());
//
//        byte[] propertyBytes3 = Arrays.copyOfRange(propertyHeaders.get(2).getBytes(), (int) beginIndex3, (int) endIndex3);
//        Property decodedProperty3 = propertiesBinaryMapper.toProperty(propertyBytes3);
//        assertThat(decodedProperty3.getKey()).isEqualTo("lastName");
//        assertThat(decodedProperty3.getType()).isEqualTo(STRING);
//        assertThat(decodedProperty3.getValue()).isEqualTo("Kowalski");
//    }
}
