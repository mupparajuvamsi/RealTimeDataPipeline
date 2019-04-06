package com.companyname.demo.springbootkafkamongodb;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.*;

@RestController
@EnableAutoConfiguration
@ComponentScan(basePackages={"com.companyname.service","com.companyname.demo.springbootkafkamongodb"})
public class Example {

	

	public static void main(String[] args) {
		SpringApplication.run(Example.class, args);
	}

}