package com.tcmp.optoII.OptoIIController;

import com.tcmp.optoII.processors.CsvWriter;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin(origins = "http://localhost:4200")
@RequestMapping("/optoii")
public class OptoIIController {

    @Autowired
    private ProducerTemplate producerTemplate;

    @Autowired
    private CsvWriter csvWriter;

    @GetMapping("/start-flow")
    public ResponseEntity<String> startFlow() {
        // Crear un nuevo Exchange para enviar al ProducerTemplate
        Exchange exchange = producerTemplate.send("direct:start", e -> {
        });

        // Obtener el contenido CSV del Exchange
        String csvContent = exchange.getMessage().getBody(String.class);

        // Construir los encabezados de la respuesta HTTP para la descarga
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "text/csv");
        headers.add("Content-Disposition", "attachment; filename=optoii.csv");

        return new ResponseEntity<>(csvContent, headers, HttpStatus.OK);
    }
}
