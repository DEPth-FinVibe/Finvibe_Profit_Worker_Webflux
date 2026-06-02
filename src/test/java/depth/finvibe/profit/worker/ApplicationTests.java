package depth.finvibe.profit.worker;

import depth.finvibe.profit.worker.application.port.out.PortfolioStateStore;
import depth.finvibe.profit.worker.application.port.out.UserStateStore;
import depth.finvibe.profit.worker.application.port.out.ValuationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class ApplicationTests {

	@MockitoBean
	private PortfolioStateStore portfolioStateStore;

	@MockitoBean
	private UserStateStore userStateStore;

	@MockitoBean
	private ValuationRepository valuationRepository;

	@Test
	void contextLoads() {
	}

}
