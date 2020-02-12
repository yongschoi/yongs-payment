package yongs.temp.model;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
@Scope(scopeName = "request", proxyMode = ScopedProxyMode.TARGET_CLASS)
public class Payment {
	private String no;
	private String company;
	private String cardNo;
	private long total;
	private long opentime;
	private String orderNo;
	
	public long getTotal() {
		return total;
	}
	public void setTotal(long total) {
		this.total = total;
	}
	public String getOrderNo() {
		return orderNo;
	}
	public void setOrderNo(String orderNo) {
		this.orderNo = orderNo;
	}
	public String getNo() {
		return no;
	}
	public void setNo(String no) {
		this.no = no;
	}
	public String getCompany() {
		return company;
	}
	public void setCompany(String company) {
		this.company = company;
	}
	public String getCardNo() {
		return cardNo;
	}
	public void setCardNo(String cardNo) {
		this.cardNo = cardNo;
	}
	public long getOpentime() {
		return opentime;
	}
	public void setOpentime(long opentime) {
		this.opentime = opentime;
	}
}
