/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.streams.serdes.avro;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import io.confluent.kafka.example.Widget;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.junit.Test;

public class ReflectionAvroSerdeSpecificTest {

  private static final String ANY_TOPIC = "any-topic";

  private static <T> ReflectionAvroSerde<T>
  createConfiguredSerdeForRecordValues(Class<T> type) {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    ReflectionAvroSerde<T> serde = new ReflectionAvroSerde<>(schemaRegistryClient, type);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  private static <T> ReflectionAvroSerde<T>
  createConfiguredSerdeForAnyValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    ReflectionAvroSerde<T> serde = new ReflectionAvroSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripRecords() {
    // Given
    ReflectionAvroSerde<Widget> serde = createConfiguredSerdeForRecordValues(Widget.class);
    Widget record = new Widget("alice");

    // When
    Widget roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, record));

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  @Test
  public void shouldRoundTripNullRecordsToNull() {
    // Given
    ReflectionAvroSerde<Widget> serde = createConfiguredSerdeForRecordValues(Widget.class);

    // When
    Widget roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC,
        serde.serializer().serialize(ANY_TOPIC, null));

    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new ReflectionAvroSerde<>(null, Widget.class);
  }

  @Test
  public void shouldRoundTripRecordsWithAbstractSuperClasses() {
    // Given
    ReflectionAvroSerde<ClassWithAbstractClassAsMember> serde = createConfiguredSerdeForRecordValues(ClassWithAbstractClassAsMember.class);
    ClassWithAbstractClassAsMember record = new ClassWithAbstractClassAsMember();
    record.inner = new A("random-id");

    byte[] serialized = serde.serializer().serialize(ANY_TOPIC, record);
    // When
    ClassWithAbstractClassAsMember roundtrippedRecord = serde.deserializer().deserialize(ANY_TOPIC, serialized);

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

  public static class ClassWithAbstractClassAsMember {

    public ClassWithAbstractClassAsMember() {}

    public ClassWithAbstractClassAsMember(Super inner) {
      this.inner = inner;
    }
    public Super inner;
  }


  public static abstract class Super { }

  public static class A extends Super {
    public String id;

    public A() {}

    public A(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      A a = (A) o;
      return Objects.equals(id, a.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  public static class ClassWithMemberClass {

    public ClassWithMemberClass() {}

    A inner;
    public ClassWithMemberClass(A inner) {
      this.inner = inner;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClassWithMemberClass that = (ClassWithMemberClass) o;
      return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }
  }

  @Test
  public void shouldRoundTripNestedClasses() {
    // Given
    ReflectionAvroSerde<ClassWithMemberClass> serde = createConfiguredSerdeForRecordValues(ClassWithMemberClass.class);
    ClassWithMemberClass record = new ClassWithMemberClass(new A("random-id"));

    byte[] serialized = serde.serializer().serialize(ANY_TOPIC, record);
    // When
    ClassWithMemberClass roundtrippedRecord = serde.deserializer().deserialize(ANY_TOPIC, serialized);

    // Then
    assertThat(roundtrippedRecord, equalTo(record));

    // Cleanup
    serde.close();
  }

}