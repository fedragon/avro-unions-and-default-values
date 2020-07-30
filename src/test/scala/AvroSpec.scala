import java.io.ByteArrayOutputStream

import org.apache.avro.{AvroTypeException, Schema}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class AvroSpec extends AnyFreeSpec with Matchers {
  "When a schema defines a union between 'null' and other type(s)" - {
    "we can assign a null value to that field" - {
      val schema =
        new Schema.Parser()
          .parse("""
                   |{
                   |  "type": "record",
                   |  "name": "example",
                   |  "fields" : [
                   |    { "name": "id", "type": "long" },
                   |    { "name": "parentId", "type": [ "null", "long" ] }
                   |  ]
                   |}""".stripMargin)
      val input = new GenericData.Record(schema)
      input.put("id", 1L)
      // this works just because parentId is implicitly null

      val json = encode(schema, input) // { "id": 1, "parentId": null }
      val output = decode(schema, schema, json)

      output.get("id") shouldBe 1L
      output.hasField("parentId") shouldBe true
      output.get("parentId") shouldBe null
    }
  }

  "When a reader schema defines a default value for a newly-added field" - {
    val writerSchema =
      new Schema.Parser()
        .parse("""
                 |{
                 |  "type": "record",
                 |  "name": "example",
                 |  "fields" : [
                 |    { "name": "id", "type": "long" }
                 |  ]
                 |}""".stripMargin)

    "we can still read data serialized using the writer schema" - {
      val readerSchema =
        new Schema.Parser()
          .parse("""
                |{
                |  "type": "record",
                |  "name": "example",
                |  "fields" : [
                |    { "name": "id", "type": "long" },
                |    { "name": "name", "type": "string", "default": "foo" }
                |  ]
                |}""".stripMargin)
      val input = new GenericData.Record(writerSchema)
      input.put("id", 1L)

      val output = decode(writerSchema, readerSchema, encode(writerSchema, input))

      output.get("id") shouldBe 1L
      output.get("name").toString shouldBe "foo"
    }

    "the same applies to union fields" - {
      val readerSchema =
        new Schema.Parser()
          .parse("""
                   |{
                   |  "type": "record",
                   |  "name": "example",
                   |  "fields" : [
                   |    { "name": "id", "type": "long" },
                   |    { "name": "parentId", "type": [ "null", "string" ], "default": null }
                   |  ]
                   |}""".stripMargin)
      val input = new GenericData.Record(writerSchema)
      input.put("id", 1L)

      val output = decode(writerSchema, readerSchema, encode(writerSchema, input))

      output.get("id") shouldBe 1L
      output.hasField("parentId") shouldBe true
      output.get("parentId") shouldBe null
    }
  }

  "Remember that the default value must have the same type as the first element in the union" - {
    val schema =
      """
        |{
        |  "type": "record",
        |  "name": "example",
        |  "fields" : [
        |    { "name": "id", "type": "long" },
        |    { "name": "nameOrId", "type": [ "null", "string" ], "default": "this wont' go well" }
        |  ]
        |}""".stripMargin

    an[AvroTypeException] shouldBe thrownBy(new Schema.Parser().parse(schema))
  }

  private def encode(schema: Schema, record: GenericRecord): String = {
    val outputStream = new ByteArrayOutputStream

    try {
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = EncoderFactory.get().jsonEncoder(schema, outputStream)
      writer.write(record, encoder)
      encoder.flush()

      outputStream.toString("utf-8")
    } finally {
      outputStream.close()
    }
  }

  private def decode(writer: Schema, reader: Schema, message: String): GenericRecord = {
    val datumReader = new GenericDatumReader[GenericRecord](writer, reader)
    val decoder = DecoderFactory.get().jsonDecoder(reader, message)

    datumReader.read(null, decoder)
  }
}
