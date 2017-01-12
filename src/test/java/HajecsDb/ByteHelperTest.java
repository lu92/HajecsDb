package HajecsDb;

import org.hajecsdb.graphs.core.Property;
import org.hajecsdb.graphs.core.PropertyType;
import org.hajecsdb.graphs.storage.ByteHelper;
import org.hajecsdb.graphs.storage.entities.BinaryProperty;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.fest.assertions.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ByteHelperTest {

    private ByteHelper byteHelper = new ByteHelper();

    @Test
    public void intValueTest() {
        Property property = new Property("id", 128, PropertyType.INT);
        BinaryProperty binaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Integer(128));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.INT);

        Property mappedProperty = byteHelper.convertBinaryFigureIntoProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Integer(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.INT);
    }

    @Test
    public void longValueTest() {
        Property property = new Property("id", 128l, PropertyType.LONG);
        BinaryProperty binaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Long(128));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.LONG);

        Property mappedProperty = byteHelper.convertBinaryFigureIntoProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Long(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.LONG);
    }

    @Test
    public void floatValueTest() {
        Property property = new Property("id", 128.0f, PropertyType.FLOAT);
        BinaryProperty binaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Float(128.0f));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.FLOAT);

        Property mappedProperty = byteHelper.convertBinaryFigureIntoProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Float(128.0f));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.FLOAT);
    }

    @Test
    public void doubleValueTest() {
        Property property = new Property("id", 128.00, PropertyType.DOUBLE);
        BinaryProperty binaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo(new Double(128.00));
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.DOUBLE);

        Property mappedProperty = byteHelper.convertBinaryFigureIntoProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo(new Double(128));
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.DOUBLE);
    }

    @Test
    public void stringValueTest() {
        Property property = new Property("id", "One hundred twenty eight", PropertyType.STRING);
        BinaryProperty binaryProperty = byteHelper.convertPropertiesIntoBinaryFigure(property);
        assertThat(binaryProperty.getKey()).isEqualTo("id");
        assertThat(binaryProperty.getValue()).isEqualTo("One hundred twenty eight");
        assertThat(binaryProperty.getType()).isEqualTo(PropertyType.STRING);

        Property mappedProperty = byteHelper.convertBinaryFigureIntoProperty(binaryProperty.getBytes());
        assertThat(mappedProperty.getKey()).isEqualTo("id");
        assertThat(mappedProperty.getValue()).isEqualTo("One hundred twenty eight");
        assertThat(mappedProperty.getType()).isEqualTo(PropertyType.STRING);
    }
}
