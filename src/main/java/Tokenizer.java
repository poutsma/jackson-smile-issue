import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import reactor.core.publisher.Flux;

/**
 * @author Arjen Poutsma
 */
final class Tokenizer {

	private final JsonParser parser;

	private final DeserializationContext deserializationContext;

	private TokenBuffer tokenBuffer;

	private int objectDepth;

	private int arrayDepth;

	private final ByteArrayFeeder inputFeeder;


	private Tokenizer(JsonParser parser, DeserializationContext deserializationContext) {

		this.parser = parser;
		this.deserializationContext = deserializationContext;
		this.tokenBuffer = new TokenBuffer(parser, deserializationContext);
		this.inputFeeder = (ByteArrayFeeder) this.parser.getNonBlockingInputFeeder();
	}


	private List<TokenBuffer> tokenize(byte[] bytes) {
		try {
			this.inputFeeder.feedInput(bytes, 0, bytes.length);
			return parseTokenBufferFlux();
		}
		catch (IOException ex) {
			throw new UncheckedIOException(ex);
		}
	}

	private Flux<TokenBuffer> endOfInput() {
		return Flux.defer(() -> {
			this.inputFeeder.endOfInput();
			try {
				return Flux.fromIterable(parseTokenBufferFlux());
			}
			catch (IOException ex) {
				throw new UncheckedIOException(ex);
			}
		});
	}

	private List<TokenBuffer> parseTokenBufferFlux() throws IOException {
		List<TokenBuffer> result = new ArrayList<>();

		// SPR-16151: Smile data format uses null to separate documents
		boolean previousNull = false;
		while (!this.parser.isClosed()) {
			JsonToken token = this.parser.nextToken();
			if (token == JsonToken.NOT_AVAILABLE ||
					token == null && previousNull) {
				break;
			}
			else if (token == null ) { // !previousNull
				previousNull = true;
				continue;
			}
			updateDepth(token);
			processTokenArray(token, result);
		}
		return result;
	}

	private void updateDepth(JsonToken token) {
		switch (token) {
			case START_OBJECT:
				this.objectDepth++;
				break;
			case END_OBJECT:
				this.objectDepth--;
				break;
			case START_ARRAY:
				this.arrayDepth++;
				break;
			case END_ARRAY:
				this.arrayDepth--;
				break;
		}
	}

	private void processTokenArray(JsonToken token, List<TokenBuffer> result) throws IOException {
		this.tokenBuffer.copyCurrentEvent(this.parser);

		if (this.objectDepth == 0 &&
				this.arrayDepth == 0 &&
				token == JsonToken.END_OBJECT) {
			result.add(this.tokenBuffer);
			this.tokenBuffer = new TokenBuffer(this.parser, this.deserializationContext);
		}
	}


	public static Flux<TokenBuffer> tokenize(Flux<byte[]> dataBuffers, JsonFactory jsonFactory,
			ObjectMapper objectMapper) {

		try {
			JsonParser parser = jsonFactory.createNonBlockingByteArrayParser();
			DeserializationContext context = objectMapper.getDeserializationContext();
			if (context instanceof DefaultDeserializationContext) {
				context = ((DefaultDeserializationContext) context).createInstance(
						objectMapper.getDeserializationConfig(), parser, objectMapper.getInjectableValues());
			}
			Tokenizer
					tokenizer = new Tokenizer(parser, context);
			return dataBuffers.concatMapIterable(tokenizer::tokenize).concatWith(tokenizer.endOfInput());
		}
		catch (IOException ex) {
			return Flux.error(ex);
		}
	}

}
