package yongs.temp.dao;

import org.springframework.data.mongodb.repository.MongoRepository;

import yongs.temp.model.Payment;

public interface PaymentRepository extends MongoRepository<Payment, String> {
	public void deleteByOrderNo(final String orderNo);
}