package yongs.temp.vo;

import yongs.temp.model.Payment;

public class Order{
	private String no; // ORD-currentTimeMillis();
	private int qty;
	private long opentime;
	private Product product;
	private User user;
	private Payment payment;
	private Delivery delivery;
	
	public String getNo() {
		return no;
	}
	public void setNo(String no) {
		this.no = no;
	}
	public int getQty() {
		return qty;
	}
	public void setQty(int qty) {
		this.qty = qty;
	}
	public long getOpentime() {
		return opentime;
	}
	public void setOpentime(long opentime) {
		this.opentime = opentime;
	}
	public Product getProduct() {
		return product;
	}
	public void setProduct(Product product) {
		this.product = product;
	}
	public User getUser() {
		return user;
	}
	public void setUser(User user) {
		this.user = user;
	}
	public Payment getPayment() {
		return payment;
	}
	public void setPayment(Payment payment) {
		this.payment = payment;
	}
	public Delivery getDelivery() {
		return delivery;
	}
	public void setDelivery(Delivery delivery) {
		this.delivery = delivery;
	}
}
