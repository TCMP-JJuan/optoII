package com.tcmp.optoII.routes;

import com.tcmp.optoII.processors.CsvWriter;
import com.tcmp.optoII.processors.optosIIRecordTransformer;
import com.tcmp.optoII.services.OptoIIService;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component

public class OptosIIRouterBuilder extends RouteBuilder {

    @Autowired
    private OptoIIService optoIIService;

    @Autowired
    private optosIIRecordTransformer optosIIRecordTransformer;

    @Autowired
    private CsvWriter csvWriter;

    @Override
    public void configure() throws Exception {
        log.info("Camel route is being initialized...");

        from("direct:start")
                .routeId("mongoServiceRoute")
                .log("Iniciando procesamiento de datos desde MongoService...")
                .bean(optoIIService, "printRealtimeData")
                .log("Datos obtenidos de MongoDB: ${body}")
                .bean(optosIIRecordTransformer)
                .log("Datos procesados con éxito desde TradeRecordTransformer.")
                .bean(csvWriter, "writeToCsv")
                .log("Datos exportados con éxito desde CsvWriter.")
                .end();
    }
}
