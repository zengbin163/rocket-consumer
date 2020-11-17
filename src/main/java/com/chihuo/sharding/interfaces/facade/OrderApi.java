package com.chihuo.sharding.interfaces.facade;

import java.math.BigDecimal;
import java.util.List;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.chihuo.sharding.application.OrderApplicationService;
import com.chihuo.sharding.domain.order.entity.Order;

@Controller
@RequestMapping("/order")
public class OrderApi {

	@Resource
	private OrderApplicationService service;
	@Value("${chihuo.apollo.first}")
	private String apolloFirst;

	private static Logger logger = LoggerFactory.getLogger(OrderApi.class);

	@RequestMapping("/add")
	@ResponseBody
	public String add() {
		Order order = new Order();
		order.setUserId(1L);
		order.setOrderAmount(BigDecimal.TEN);
		order.setHongbaoAmount(BigDecimal.TEN);
		this.service.saveOrder(order);
		return "success";
	}

	@RequestMapping("/orderList")
	@ResponseBody
	public List<Order> orderList(@RequestParam(value = "current") Integer current,
			@RequestParam(value = "size") Integer size) {
		return this.service.findOrderList(current, size);
	}

	@RequestMapping("/health")
	@ResponseBody
	public String health() {
        logger.debug( "debug log..." );
        logger.info( "info log..." );
        logger.warn( "warn log..." );

		return "health chech { " + apolloFirst + " }";
	}

}
