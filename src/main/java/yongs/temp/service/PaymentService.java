package yongs.temp.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import yongs.temp.dao.PaymentRepository;
import yongs.temp.vo.Order;

@Service
public class PaymentService {
	private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);
	// for sender
	private static final String PAYMENT_DELIVERY_EVT = "payment-to-delivery";
	private static final String PAYMENT_ROLLBACK_EVT = "payment-rollback";
	
	// for listener
	private static final String STOCK_PAYMENT_EVT = "stock-to-payment";
	private static final String ROLLBACK_EVT = "delivery-rollback";
	
	@Autowired
    KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
    PaymentRepository repo;

	@KafkaListener(topics = STOCK_PAYMENT_EVT)
	public void create(String orderStr) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		Order order = mapper.readValue(orderStr, Order.class);
		
		long curr = System.currentTimeMillis();
		String no = "PAY" + curr;
		order.getPayment().setNo(no);
		order.getPayment().setOpentime(curr);
		order.getPayment().setOrderNo(order.getNo());
		
		try {
			// 결제 API call
			repo.insert(order.getPayment());
			String newOrderStr = mapper.writeValueAsString(order);
			logger.info(">>>>> 결제 성공  >>>>>> " + order.getNo());
			kafkaTemplate.send(PAYMENT_DELIVERY_EVT, newOrderStr);
		} catch (Exception e) {
			logger.info(">>>>> 결제 실패  >>>>>> " + order.getNo());
			kafkaTemplate.send(PAYMENT_ROLLBACK_EVT, orderStr);			
		}
	}
	
	@KafkaListener(topics = ROLLBACK_EVT)
	public void rollback(String orderStr) throws JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		Order order = mapper.readValue(orderStr, Order.class);
		
		// Payment 데이터 삭제 
		repo.deleteByOrderNo(order.getNo()); 
		logger.info("Payment No [" + order.getPayment().getNo() + "] Rollback !!!");
	}

}
