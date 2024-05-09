package com.ibm.sample.cliente.bff;

import java.net.URI;
import java.util.List;
import java.util.Map.Entry;

import org.apache.kafka.common.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import jdk.internal.org.jline.utils.Log;

import com.ibm.sample.HttpHeaderInjectAdapter;
import com.ibm.sample.KafkaHeaderMap;
import com.ibm.sample.cliente.bff.dto.Cliente;
import com.ibm.sample.cliente.bff.dto.RespostaBFF;
import com.ibm.sample.cliente.bff.dto.RetornoCliente;


@RestController
public class ClienteBFFRest {

	@Value("${cliente-rest.url}")
	private String urlClienteRest; 
	
	Logger logger = LoggerFactory.getLogger(ClienteBFFRest.class);
	
	@Value("${delete-cliente-kafka-topico}")
	private String deleteTopic; 

	@Value("${cliente-kafka-topico}")
	private String cadastroTopic; 

	@Value("${cliente-update-topico}")
	private String updateTopic;
	
	@Autowired
	private KafkaTemplate<String, Cliente> kafka;
	
	@Autowired
	private RestTemplate clienteRest;
	
	@Autowired
	private Tracer tracer;
	
	@CrossOrigin(origins = "*")
	@GetMapping("/bff/cliente/pesquisa/{nome}")
	public List<Cliente> pesquisaClientes(@PathVariable String nome)
	{	
		Span span = tracer.buildSpan("pesquisaCliente").start();
		span.setTag("pesquisa", nome);
		logger.debug("[pesquisaClientes] " + nome);
		logger.info("It will search for customers with name starting with : " + nome);
		org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
		HttpHeaders httpHeaders = new HttpHeaders();
		HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
		tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
		HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
		Object[] param = new Object[0];
		List<Cliente> resultado = clienteRest.exchange(urlClienteRest+"/pesquisa/" + nome, org.springframework.http.HttpMethod.GET, entity, List.class, param).getBody();
		if (resultado!=null)
		{
			logger.debug("Found: " + resultado.size() + " customer(s) in the result");
		}
		span.finish();
		return resultado;
	}
	
	@CrossOrigin(origins = "*")
	@GetMapping("/bff/cliente/{cpf}")
	public ResponseEntity<RetornoCliente> recuperaCliente(@PathVariable Long cpf)
	{
		Span span = tracer.buildSpan("recuperaCliente").start();
		span.setTag("cpf", cpf);
		logger.debug("[recuperaCliente] " + cpf);
		try
		{
			logger.debug("It will search a customer for ID: " + cpf);
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			Object[] param = new Object[0];
			RetornoCliente retorno = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			if (retorno!=null && logger.isDebugEnabled())
			{
				logger.debug("Search result: " + retorno.getMensagem());
				if (retorno.getCliente()!=null)
				{
					logger.debug("Customer: " + retorno.getCliente().getNome());
				}
			}
			else{
				logger.info("Customer not found with the ID: " + cpf);
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
		    return ResponseEntity.ok(retorno);
		}
		catch (Exception e)
		{
			logger.warn("Error to search customer by ID: " + e.getMessage());
			span.setTag("error",true);
			span.setTag("ErrorMessage", e.getMessage());
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
			
		}
		finally{
			span.finish();
		}
		
	}
	
	@CrossOrigin(origins = "*")
	@DeleteMapping("/bff/cliente/{cpf}")
	public ResponseEntity<RespostaBFF> excluiCliente(@PathVariable Long cpf)
	{
		Span span = tracer.buildSpan("excluirCliente").start();
		span.setTag( "cpf",cpf);
		logger.debug("[excluiCliente] " + cpf);
		RespostaBFF resposta = new RespostaBFF();
		
		try
		{
			logger.debug("It will check if the customer exists!");		
			if (!clienteExiste(span, cpf))
			{
				logger.warn("Customer requested to be deleted does not exist with this ID: " + cpf);
				span.log("Customer requested to be deleted does not exist with this ID: " + cpf);
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}	
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			//Object[] param = new Object[0];
			//RetornoCliente retorno = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			logger.debug(cliConsulta.getMensagem());
			logger.debug("Sending message for Kafka Topic to customer be deleted async");
			enviaMensagemKafka(span, this.deleteTopic, cliConsulta.getCliente());
			logger.debug("Message sent to Kafka");
			resposta.setCodigo("202-DELETED");
			resposta.setMensagem("Customer Deletion submited successfully! Customer: " + cliConsulta.getCliente().toString() );
			logger.info(resposta.getCodigo() + " - " + resposta.getMensagem());
			//Thread.sleep(3000);
			return ResponseEntity.ok(resposta);
		}
		catch (Exception e)
		{
			logger.warn("Error to delete customer: "  + e.getMessage());
			span.setTag("error",true);
			span.setTag("ErrorMessage", e.getMessage());
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		finally{
			span.finish();
		}
		
	}
	
	@CrossOrigin(origins = "*")
	@PostMapping("/bff/cliente")
	public ResponseEntity<RespostaBFF> processaCadastro(@RequestBody Cliente cliente)
	{
		Span span = tracer.buildSpan("cadastraCliente").start();
		logger.debug("[processaCadastro] " + cliente.getNome());
		RespostaBFF resposta = new RespostaBFF();
		
		try
		{
			logger.debug("Validating the data sent to create a customer record");
			this.validaCliente(cliente);
			logger.debug("Customer data validated successfully, verifying if the customer already exists in the database");
			if (this.clienteExiste(span,cliente.getCpf()))
			{
				logger.info("Customer exists in the database, operation canceled to avoid data duplication. ID:" + cliente.getCpf());
				return new ResponseEntity<>(HttpStatus.ALREADY_REPORTED);
			} 
			logger.debug("Sending a message to Kafka topic to be process async");
			enviaMensagemKafka(span, this.cadastroTopic, cliente);
			logger.debug("Message sent to kafka Topic");
		
			resposta.setCodigo("200-SUCCESS");
			resposta.setMensagem("Customer creation submited successfully! Customer: " + cliente.toString() );
			logger.info(resposta.getCodigo() + " - " + resposta.getMensagem());
			return ResponseEntity.ok(resposta);
		}
		catch (Exception e)
		{
			logger.error("Error to create a new customer: " + cliente.toString() + ", error: "  + e.getMessage());
			span.setTag("error",true);
			span.setTag("ErrorMessage", e.getMessage());
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		finally{
			span.finish();
		}
	}

	@CrossOrigin(origins = "*")
	@PutMapping("/bff/cliente")
	public ResponseEntity<RespostaBFF> atualizaCadastro(@RequestBody Cliente cliente)
	{
		Span span = tracer.buildSpan("cadastraCliente").start();
		logger.debug("[processaCadastro] " + cliente.getNome());
		RespostaBFF resposta = new RespostaBFF();
		
		try
		{
			logger.debug("Validating the data sent to create a customer record");
			this.validaCliente(cliente);
			logger.debug("Customer data validated successfully, verifying if the customer already exists in the database");
			if (!this.clienteExiste(span,cliente.getCpf()))
			{
				logger.info("Customer not found in the database, operation canceled. ID:" + cliente.getCpf());
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			} 
			logger.debug("Sending a message to Kafka topic to be process async");
			enviaMensagemKafka(span, this.updateTopic, cliente);
			logger.debug("Message sent to kafka Topic");
		
			resposta.setCodigo("200-SUCCESS");
			resposta.setMensagem("Customer update submited successfully! Customer: " + cliente.toString() );
			logger.info(resposta.getCodigo() + " - " + resposta.getMensagem());
			return ResponseEntity.ok(resposta);
		}
		catch (Exception e)
		{
			logger.error("Error to update customer data: " + cliente.toString() + ", error: "  + e.getMessage());
			span.setTag("error",true);
			span.setTag("ErrorMessage", e.getMessage());
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}
		finally{
			span.finish();
		}
	}	
	private void enviaMensagemKafka(Span spanPai,String topico, Cliente cliente)
	{
		KafkaHeaderMap h1 = new KafkaHeaderMap();
		Span span = null;
		if (spanPai!=null)
		{
			span = tracer.buildSpan("envioMensagemKafka-" + topico).asChildOf(spanPai).start();
			tracer.inject(span.context(), Format.Builtin.TEXT_MAP, h1);
			span.setTag("kafka.mensagem", cliente.toString());
			span.setTag("kafka.topico", topico); 
			span.setTag("span.kind", "KafkaProducer");
		}
		Entry<String, String> item = h1.getContext();
		org.springframework.messaging.Message<Cliente> mensagem = MessageBuilder
				.withPayload(cliente)
				.setHeader(KafkaHeaders.TOPIC, topico)
				.setHeader("tracer_context_" + item.getKey(), item.getValue())
				.build();
		kafka.send(mensagem);
		logger.debug("Mensage: " + cliente + " sent to topic:  " + topico);
		if (spanPai!=null)
		{
			span.finish();
		}
	}
	private RetornoCliente cliConsulta=null;
	private boolean clienteExiste(Span spanPai, Long cpf) throws Exception
	{
		Span span = tracer.buildSpan("verificaClienteExiste").asChildOf(spanPai).start();
		try
		{
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			Object[] param = new Object[0];
			cliConsulta = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			if (cliConsulta!=null && cliConsulta.getCodigo()!=null && cliConsulta.getCodigo().equals("200-FOUND"))
			{
				return true;
			}
			return false;
		}
		catch (Exception e)
		{
			//throw new Exception("Error to Search for a customer! " + e.getMessage());
		}
		finally {
			span.finish();
		}
		return false;
	}
	
	private void validaCliente(Cliente cliente) throws Exception
	{
		if (cliente==null)
		{
			throw new Exception("Payload invalid, it wasn't found customer data");
		}
		if (cliente.getCpf()==null || cliente.getCpf()==0)
		{
			throw new Exception("CPF is a required field");
		}
		if (cliente.getNome()==null || cliente.getNome().length()==0)
		{
			throw new Exception("Nome is a required field");
		}
		
	}
	
}
 
