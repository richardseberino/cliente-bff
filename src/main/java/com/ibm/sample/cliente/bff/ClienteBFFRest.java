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
		logger.info("vai peesquisar clientes com o nome contendo: " + nome);
		org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
		Tags.SPAN_KIND.set(tracer.activeSpan(), Tags.SPAN_KIND_CLIENT);
  		Tags.HTTP_METHOD.set(tracer.activeSpan(), "GET");
		Tags.HTTP_URL.set(tracer.activeSpan(), urlClienteRest+"/pesquisa/" + nome);
		HttpHeaders httpHeaders = new HttpHeaders();
		HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
		tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
		HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
		Object[] param = new Object[0];
		List<Cliente> resultado = clienteRest.exchange(urlClienteRest+"/pesquisa/" + nome, org.springframework.http.HttpMethod.GET, entity, List.class, param).getBody();
		if (resultado!=null)
		{
			logger.debug("Encontrado: " + resultado.size() + " clientes na pesuisa");
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
			logger.debug("Vai pesquisar o cliente pelo cpf " + cpf);
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			Object[] param = new Object[0];
			RetornoCliente retorno = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			if (retorno!=null && logger.isDebugEnabled())
			{
				logger.debug("resultado da busca: " + retorno.getMensagem());
				if (retorno.getCliente()!=null)
				{
					logger.debug("Cliente: " + retorno.getCliente().getNome());
				}
			}
			else{
				logger.info("Não encontrado cliente pelo CPF " + cpf);
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}
		    return ResponseEntity.ok(retorno);
		}
		catch (Exception e)
		{
			logger.warn("Falha na pesquisa de cliente pelo CPF " + e.getMessage());
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
			logger.debug("vai pesquisar se o cliente existe!");		
			if (!clienteExiste(span, cpf))
			{
				logger.warn("Cliente não existe para ser excluido: cpf " + cpf);
				span.log("Cliente não existe para ser excluido: cpf " + cpf);
				return new ResponseEntity<>(HttpStatus.NOT_FOUND);
			}	
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			Object[] param = new Object[0];
			RetornoCliente retorno = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			logger.debug(retorno.getMensagem());
			logger.debug("Enviando mensagem para o topico Kafka para realizar a exclusao de forma asyncrona");
			enviaMensagemKafka(span, this.deleteTopic, retorno.getCliente());
			logger.debug("Mensagem enviada para o kafka");
			resposta.setCodigo("202-EXCLUIDO");
			resposta.setMensagem("Deleção submetida com sucesso! cliente: " + retorno.getCliente().toString() );
			logger.info(resposta.getCodigo() + " - " + resposta.getMensagem());
			return ResponseEntity.ok(resposta);
		}
		catch (Exception e)
		{
			logger.warn("Problemas durante a exclusão do cliente: "  + e.getMessage());
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
			logger.debug("Validando se os dados informados para cadastro estão corretos");
			this.validaCliente(cliente);
			logger.debug("Dados validados com sucesso, verificando se o cliente já existe na base de dados");
			if (this.clienteExiste(span,cliente.getCpf()))
			{
				logger.info("CLiente já existe na base de dados, cadastro abortado para evitar duplicidade. CLiente CPF: " + cliente.getCpf());
				return new ResponseEntity<>(HttpStatus.ALREADY_REPORTED);
			} 
			logger.debug("Vai enviar a mensagem para o topico Kafka para processamento do cadastro de forma assíncrona");
			enviaMensagemKafka(span, this.cadastroTopic, cliente);
			logger.debug("Mensagem enviada com sucesso ao topico kafka");
		
			resposta.setCodigo("200-SUCESSO");
			resposta.setMensagem("Cadastro submetido com sucesso! cliente: " + cliente.toString() );
			logger.info(resposta.getCodigo() + " - " + resposta.getMensagem());
			return ResponseEntity.ok(resposta);
		}
		catch (Exception e)
		{
			logger.error("Falha durante o cadastro do cliente: " + cliente.toString() + ", erro: "  + e.getMessage());
			span.setTag("error",true);
			span.setTag("ErrorMessage", e.getMessage());
			return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
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
		logger.debug("Mensagem: " + cliente + " enviada para o topico:  " + topico);
		if (spanPai!=null)
		{
			span.finish();
		}
	}
	
	private boolean clienteExiste(Span spanPai, Long cpf)
	{
		Span span = tracer.buildSpan("verificaClienteExiste").asChildOf(spanPai).start();
		try
		{
			HttpHeaders httpHeaders = new HttpHeaders();
			HttpHeaderInjectAdapter h1 = new HttpHeaderInjectAdapter(httpHeaders);
			tracer.inject(span.context(), Format.Builtin.HTTP_HEADERS,h1);
			HttpEntity<String> entity = new HttpEntity<>(h1.getHeaders());
			Object[] param = new Object[0];
			RetornoCliente resultado = clienteRest.exchange(urlClienteRest+"/" + cpf, HttpMethod.GET,entity, RetornoCliente.class, param).getBody();
			if (resultado.getCodigo().equals("200-FOUND"))
			{
				return true;
			}
		}
		catch (Exception e)
		{
			
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
			throw new Exception("Payload inváido, não foram encontrados os dados do cliente");
		}
		if (cliente.getCpf()==null || cliente.getCpf()==0)
		{
			throw new Exception("CPF é um campo obrigatório");
		}
		if (cliente.getNome()==null || cliente.getNome().length()==0)
		{
			throw new Exception("Nome é um campo obrigatório");
		}
		
	}
	
}
 