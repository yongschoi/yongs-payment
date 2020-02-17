package yongs.temp.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

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
	
	@Autowired
    KafkaTemplate<String, String> kafkaTemplate;
	
	@Autowired
    PaymentRepository repo;

	@KafkaListener(topics = STOCK_PAYMENT_EVT)
	public void create(String orderStr, Acknowledgment ack) {
		ObjectMapper mapper = new ObjectMapper();
	
		try {
			Order order = mapper.readValue(orderStr, Order.class);
			
			long curr = System.currentTimeMillis();
			String no = "PAY" + curr;
			order.getPayment().setNo(no);
			order.getPayment().setOpentime(curr);
			order.getPayment().setOrderNo(order.getNo());
			
			// 결제 API call
			// ...
			// ...
			repo.insert(order.getPayment());
			String newOrderStr = mapper.writeValueAsString(order);
			kafkaTemplate.send(PAYMENT_DELIVERY_EVT, newOrderStr);
			logger.debug("[PAYMENT to DELIVERY(결제성공)] Order No [" + order.getNo() + "]");
		} catch (Exception e) {
			kafkaTemplate.send(PAYMENT_ROLLBACK_EVT, orderStr);
			logger.debug("[PAYMENT Exception(결제 실패)]");
		}
		// 성공하든 실패하든
		ack.acknowledge();
	}
	
	@KafkaListener(topics = {"delivery-rollback"})
	public void rollback(String orderStr, Acknowledgment ack) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			Order order = mapper.readValue(orderStr, Order.class);
			
			// Payment 데이터 삭제 
			repo.deleteByOrderNo(order.getNo()); 		
			// 성공할때 까지
			ack.acknowledge();
			logger.debug("[PAYMENT Rollback] Order No [" + order.getNo() + "]");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
