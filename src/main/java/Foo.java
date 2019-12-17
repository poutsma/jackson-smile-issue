import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = Foo.Serializer.class)
@JsonDeserialize(using = Foo.Deserializer.class)
public class Foo {

	private String bar;

	public Foo(String bar) {
		this.bar = bar;
	}

	@Override
	public String toString() {
		return this.bar;
	}

	public static class Serializer extends JsonSerializer<Foo> {

		@Override
		public void serialize(Foo value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
			gen.writeStartObject();
			for (int i = 0; i < 10; i++) {
				ThreadLocalRandom random = ThreadLocalRandom.current();
				gen.writeFieldName("name" + random.nextInt(2));
				gen.writeNumber(i);
			}
			gen.writeEndObject();
		}
	}

	public static class Deserializer extends JsonDeserializer<Foo> {

		@Override
		public Foo deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
			if (p.currentToken() != JsonToken.START_OBJECT) {
				assert p.nextValue() == JsonToken.START_OBJECT;
			}
			StringBuilder b = new StringBuilder();
			while (p.nextToken() != JsonToken.END_OBJECT) {
				b.append(p.currentName());
				b.append('=');
				b.append(p.nextIntValue(-1));
			}
			return new Foo(b.toString());
		}
	}
}

