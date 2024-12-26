package com.tcmp.optoII.processors;


import com.tcmp.optoII.model.OptosIIRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;

@Component
@Slf4j
public class optosIIRecordTransformer implements Processor {

    @Override
    public void process(Exchange exchange) throws Exception {
        // Obtener el cuerpo del mensaje (lista de documentos de MongoDB)
        HashMap<String,List<Document>> data =  new HashMap<>();
        data = exchange.getIn().getBody(data.getClass());
        List<Document> tradeDocs = data.get("RealTime");
        List<Document> SensitivityDocs =  data.get("Sensitivities");

        //List<Document> data = exchange.getIn().getBody(List.class); // Lista de documentos

        log.info("Datos recibidos: {}", data);

        // Validar que la lista no sea nula o vacía

        if (data == null || data.isEmpty()) {
            log.warn("No se encontraron datos en el cuerpo del mensaje.");
            exchange.getIn().setBody(new ArrayList<>()); // Retornar una lista vacía
            return;
        }

        // Crear una lista para almacenar los objetos TradeRecord
        List<OptosIIRecord> optoIIRecords = new ArrayList<>();

        // Procesar cada documento
        int index=0;
        for (Document tradeDoc : tradeDocs) {
            try {
                Document SensitivityDoc = SensitivityDocs.get(index);

                log.info("Procesando documento: {}", tradeDoc.toJson());

                OptosIIRecord optosIIRecord = new OptosIIRecord();
                //Obtner valores desde los documentos


                // Set fixed fields
                optosIIRecord.setInst("040044");
                optosIIRecord.setOficina("R");
                optosIIRecord.setQuanto("N");
                optosIIRecord.setTcQuant("0");

                // Set dynamic fields
                optosIIRecord.setContrapar(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "parties", "counterparty", "partyLei")));
                optosIIRecord.setFeConOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeDate")));
                optosIIRecord.setFeIniOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "startDate")));
                optosIIRecord.setFeVenOpe(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "endDate")));

//                Integer fechas = calcularDiasHabiles(
//                        LocalDate.parse(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "startDate"))),
//                        LocalDate.parse(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "endDate")))
//                );

                optosIIRecord.setDiasLiq(0);
                optosIIRecord.setPosicion(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "buySell")));
                optosIIRecord.setTipOpc(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "optionType")));
                optosIIRecord.setOpcLiq(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "exerciseStyle", "optionExerciseStyle")));
                optosIIRecord.setObjetivo(determineObjetivo(tradeDoc));
                optosIIRecord.setImpBase(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "underlyingAmount")));
                optosIIRecord.setMdaImp(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "underlyingCurrencyCode")));
                optosIIRecord.setLiquida(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "settlement", "deliveryMethod")));
                optosIIRecord.setMdaLiquida(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "payOffCurrency")));
                optosIIRecord.setPrima(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "premiumPaymentAmount")));
                optosIIRecord.setMdaPrima(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "premiumPaymentCurrency")));
                optosIIRecord.setFePrim(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "premiumPaymentDate")));
                optosIIRecord.setCveTit(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "underlyingInstrumentName")));
                optosIIRecord.setPrecioEjer(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "strikeRate")));
                optosIIRecord.setMdaPrecio(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "quoteCurrency")));
                optosIIRecord.setPaqEst("90"); //Duda
                optosIIRecord.setIdPaqEst(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeIdentifiers", "tradePackageId")));
                optosIIRecord.setConPaqEst(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "tradeIdentifiers", "tradePackageId"))); //Duda
                optosIIRecord.setSuby(getEmbeddedString(tradeDoc,List.of("TradeMessage","trade", "product", "underlyingInstrumentName")));
                optosIIRecord.setIntEje("0");
                optosIIRecord.setIntMon(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "asianFeature", "calculationFrequencyType", "periodMultiplier")));
                optosIIRecord.setNuToEje("1"); //Duda
                optosIIRecord.setNumIdOpSby("    ");//Duda
                optosIIRecord.setNumSuby("0"); //Duda
                optosIIRecord.setMdaSuby(getEmbeddedString(tradeDoc,List.of("TradeMessage", "trade", "product","underlyingCurrencyCode")));
                optosIIRecord.setPreSup(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "barrierFeature", "barrierUpRate"))); //Null en Narendra
                optosIIRecord.setPreInf(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "barrierFeature", "barrierDownRate")));//Null en Narendra
                optosIIRecord.setTipDer(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "barrierFeature", "knocktype")));//Null en Narendra
                optosIIRecord.setRevOp(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "sourceSystemProductId", "sourceInstrumentCategory")));
                optosIIRecord.setBroker(getEmbeddedString(tradeDoc,List.of("TradeMessage","trade","parties", "executingBroker", "partyName")));
                optosIIRecord.setSocioLiq("Unicamente operaciones OTC"); //Duda
                optosIIRecord.setCamCom("Unicamente operaciones OTC"); //Duda
                optosIIRecord.setAgCal("202638"); //Duda
                optosIIRecord.setNumConf("2107426"); //Duda
                optosIIRecord.setDelta("0.00"); //Duda
                optosIIRecord.setNumId(getEmbeddedString(tradeDoc,List.of("TradeMessage", "trade", "tradeIdentifiers", "originatingTradeId", "id")));
                optosIIRecord.setInstLei(getEmbeddedString(tradeDoc,List.of("TradeMessage", "trade", "parties", "counterparty", "partyLei")));
                optosIIRecord.setUti(getEmbeddedString(tradeDoc,List.of("TradeMessage", "trade","tradeHeader", "tradeIdentifiers", "uniqueTransactionId")));
                optosIIRecord.setUpi("N/A");
                optosIIRecord.setIdentificador("ON"); //Duda
                optosIIRecord.setModPre(getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "product", "barrierFeature", "knocktype")));




                // Agregar el TradeRecord a la lista
                optoIIRecords.add(optosIIRecord);
                index++;
            } catch (Exception e) {
                log.error("Error procesando el documento: {}", tradeDoc.toJson(), e);
            }
        }

        // Establecer la lista de TradeRecord como el cuerpo del mensaje
        exchange.getIn().setBody(optoIIRecords);
    }

    // Métodos auxiliares
    private static String determineObjetivo(Document tradeDoc) {
        String hedgeType = getEmbeddedString(tradeDoc, List.of("TradeMessage", "trade", "tradeHeader", "regulatory", "isHedgeTrade"));
        return hedgeType != null && !hedgeType.isEmpty() ? "COBERTURA" : "NEGOCIACION";
    }

    public static int calcularDiasHabiles(LocalDate startDate, LocalDate endDate) {

        int diasHabiles = 0;

        while (!startDate.isAfter(endDate)) {

            if (startDate.getDayOfWeek() != DayOfWeek.SATURDAY && startDate.getDayOfWeek() != DayOfWeek.SUNDAY) {

                diasHabiles++;

            }

            startDate = startDate.plusDays(1);

        }

        return diasHabiles;

    }



    private static String getEmbeddedString(Document doc, List<String> path) {
        try {
            Object value = doc;
            for (String key : path) {
                if (value instanceof Map) {
                    value = ((Map<?, ?>) value).get(key);
                } else {
                    return "N/A";
                }
            }
            return value != null ? value.toString() : "N/A";
        } catch (Exception e) {
            return "N/A";
        }
    }



    private Double getEmbeddedDouble(Document doc, List<String> path) {
        Object value = doc.getEmbedded(path, Object.class);
        if (value instanceof Double) {
            return (Double) value;
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                log.warn("No se pudo convertir el valor '{}' a Double en la ruta {}", value, path, e);
            }
        }
        log.warn("Valor no compatible encontrado en la ruta {}: {}", path, value);
        return null;
    }


    private int getEmbeddedInteger(Document doc, List<String> path) {
        try {
            Object value = doc.getEmbedded(path, Object.class);  // Get the first part of the path
            if (value == null) {
                return -1; // Return default value if no value found
            }
            if (value instanceof Integer) {
                return (int) value;
            }
            if (value instanceof String) {
                return Integer.parseInt((String) value); // Try converting from String
            }
            return 0; // Return default value if conversion fails
        } catch (Exception e) {
            log.warn("Error extracting double from path {}: {}", path, e.getMessage());
            return 0; // Return default value if any error occurs
        }
    }
}
