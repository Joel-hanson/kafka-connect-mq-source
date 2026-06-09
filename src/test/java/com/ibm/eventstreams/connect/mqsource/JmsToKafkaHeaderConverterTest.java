/**
 * Copyright 2019, 2024 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.ibm.eventstreams.connect.mqsource;

import com.ibm.eventstreams.connect.mqsource.processor.JmsToKafkaHeaderConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JmsToKafkaHeaderConverterTest {

    @Mock
    private TextMessage message;
    @InjectMocks
    private JmsToKafkaHeaderConverter jmsToKafkaHeaderConverter;

    @Test
    public void convertJmsPropertiesToKafkaHeaders() throws JMSException {

        final List<String> keys = Arrays.asList("facilityCountryCode", "facilityNum", "nullProperty");

        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("facilityCountryCode")).thenReturn("US");
        when(message.getObjectProperty("facilityNum")).thenReturn("12345");
        when(message.getObjectProperty("nullProperty")).thenReturn(null);

        // Act
        final ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter
                .convertJmsPropertiesToKafkaHeaders(message);


        //Verify
        assertEquals("All three custom JMS properties were copied to kafka successfully.", 3, actualConnectHeaders.size());
    }

    @Test
    public void convertIntegerJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that Integer JMS properties remain as integers in Kafka headers
        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_Priority", "customIntProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_Priority")).thenReturn(5);
        when(message.getObjectProperty("customIntProp")).thenReturn(12345);

        // Act
        final ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter
                .convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());
        
        Header priorityHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_Priority");
        assertEquals(Schema.Type.INT32, priorityHeader.schema().type());
        assertEquals(5, priorityHeader.value());

        Header customIntHeader = actualConnectHeaders.lastWithName("customIntProp");
        assertEquals(Schema.Type.INT32, customIntHeader.schema().type());
        assertEquals(12345, customIntHeader.value());
    }

    @Test
    public void convertByteArrayJmsPropertiesToKafkaHeaders() throws JMSException {
         // Test that type byte array is preserved when converting JMS properties to Kafka headers
        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_MsgId", "JMS_IBM_MQMD_CorrelId");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);
        
        final byte[] msgId = new byte[]{0x01, 0x02, 0x03, 0x04};
        final byte[] correlId = new byte[]{0x05, 0x06, 0x07, 0x08};

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_MsgId")).thenReturn(msgId);
        when(message.getObjectProperty("JMS_IBM_MQMD_CorrelId")).thenReturn(correlId);

        // Act
        final ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter
                .convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());
        
        Header msgIdHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_MsgId");
        assertEquals(Schema.Type.BYTES, msgIdHeader.schema().type());
        assertArrayEquals(msgId, (byte[]) msgIdHeader.value());

        Header correlIdHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_CorrelId");
        assertEquals(Schema.Type.BYTES, correlIdHeader.schema().type());
        assertArrayEquals(correlId, (byte[]) correlIdHeader.value());
    }

    @Test
    public void convertStringJmsPropertiesToKafkaHeaders() throws JMSException {
        // Test that String properties remain as strings
        final List<String> keys = Arrays.asList("JMS_IBM_MQMD_Format", "customStringProp");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("JMS_IBM_MQMD_Format")).thenReturn("MQSTR");
        when(message.getObjectProperty("customStringProp")).thenReturn("testValue");

        // Act
        final ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter
                .convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(2, actualConnectHeaders.size());
        
        Header formatHeader = actualConnectHeaders.lastWithName("JMS_IBM_MQMD_Format");
        assertEquals(Schema.Type.STRING, formatHeader.schema().type());
        assertEquals("MQSTR", formatHeader.value());

        Header customHeader = actualConnectHeaders.lastWithName("customStringProp");
        assertEquals(Schema.Type.STRING, customHeader.schema().type());
        assertEquals("testValue", customHeader.value());
    }

    @Test
    public void NullValuesInJmsPropertiesToKafkaHeaders() throws JMSException {
        final List<String> keys = Arrays.asList("nullProperty");
        final Enumeration<String> keyEnumeration = Collections.enumeration(keys);

        // Arrange
        when(message.getPropertyNames()).thenReturn(keyEnumeration);
        when(message.getObjectProperty("nullProperty")).thenReturn(null);

        // Act
        final ConnectHeaders actualConnectHeaders = jmsToKafkaHeaderConverter
                .convertJmsPropertiesToKafkaHeaders(message);

        // Verify
        assertEquals(1, actualConnectHeaders.size());
        Header nullHeader = actualConnectHeaders.lastWithName("nullProperty");
        assertEquals(Schema.Type.STRING, nullHeader.schema().type());
        assertNull(nullHeader.value());
    }
}