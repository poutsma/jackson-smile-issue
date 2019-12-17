import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactoryBuilder;
import reactor.core.publisher.Flux;

import static java.time.Duration.of;
import static java.time.temporal.ChronoUnit.MICROS;

public class Driver {

	public static void main(String[] args) throws Exception {
		Flux<Foo> source = Flux.interval(of(100, MICROS))
				.take(30000)
				.map(l -> new Foo(String.valueOf(l)));

		SmileFactoryBuilder builder = new SmileFactoryBuilder(new SmileFactory());
		SmileFactory factory = builder.build();
		ObjectMapper mapper = new ObjectMapper(factory);

		Flux<byte[]> serialized = source.map(to -> {
			try {
				return mapper.writeValueAsBytes(to);
			}
			catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		});

		Flux<TokenBuffer> tokens = Tokenizer.tokenize(serialized, factory, mapper);

		CountDownLatch latch = new CountDownLatch(1);
		tokens.subscribe(null,
				throwable -> {
					throwable.printStackTrace();
					latch.countDown();
				},
				latch::countDown);

		latch.await();

	}

}
