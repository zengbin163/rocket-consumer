package com.chihuo.sharding.application;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.chihuo.sharding.domain.order.entity.Order;
import com.chihuo.sharding.domain.order.service.OrderDomainService;

@Service
public class OrderApplicationService {
	
	@Autowired
	private OrderDomainService orderDomainService;
	
    public Long saveOrder(Order order) {
    	return this.orderDomainService.saveOrder(order);
    }

	public List<Order> findOrderList(Integer current, Integer size) {
		return this.orderDomainService.findOrderList(current, size);
	}
	
	public Integer findOrderCount() {
		return this.orderDomainService.countOrderList();
	}
	
}
