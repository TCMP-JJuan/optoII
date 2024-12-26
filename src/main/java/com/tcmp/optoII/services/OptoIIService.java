package com.tcmp.optoII.services;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class OptoIIService {

    @Autowired
    private MongoTemplate mongoTemplate;

    public void printRealtimeData(Exchange exchange) {
        // Obtener la base de datos
        MongoDatabase database = mongoTemplate.getDb();
        MongoCollection<Document> collection1 = database.getCollection("Sensitivity");
        MongoCollection<Document> collection2 = database.getCollection("Realtime");

        // Crear la proyección de los campos que queremos obtener
        Document projection1 = new Document("_id", 1)
                .append("SensitivityMessage.sensitivityData.scenarioDefinition.scenarioOutputs.riskFactor1Value", 1);

        Document projection2 = new Document("TradeMessage.header.batchId", 1)
                .append("TradeMessage.trade.parties.counterparty.partyLei", 1)
                .append("TradeMessage.trade.tradeHeader.tradeDate", 1)
                .append("TradeMessage.trade.product.startDate", 1)
                .append("TradeMessage.trade.product.endDate", 1)
                .append("TradeMessage.trade.product.buySell", 1)
                .append("TradeMessage.trade.product.optionType", 1)
                .append("TradeMessage.trade.product.exerciseStyle.optionExerciseStyle", 1)
                .append("TradeMessage.trade.tradeHeader.regulatory.isHedgeTrade", 1)
                .append("TradeMessage.trade.product.underlyingAmount", 1)
                .append("TradeMessage.trade.product.underlyingCurrencyCode", 1)
                .append("TradeMessage.trade.tradeHeader.settlement.deliveryMethod", 1)
                .append("TradeMessage.trade.product.payOffCurrency", 1)
                .append("TradeMessage.trade.product.premiumPaymentAmount", 1)
                .append("TradeMessage.trade.product.premiumPaymentCurrency", 1)
                .append("TradeMessage.trade.product.premiumPaymentDate", 1)
                .append("TradeMessage.trade.tradeHeader.tradeIdentifiers.tradePackageId", 1)
                .append("TradeMessage.trade.product.underlyingInstrumentName", 1)
                .append("TradeMessage.trade.product.asianFeature.calculationFrequencyType.periodMultiplier", 1)
                .append("TradeMessage.trade.product.strikeRate", 1)
                .append("TradeMessage.trade.product.quoteCurrency", 1)
                .append("TradeMessage.trade.product.barrierFeature.barrierUpRate", 1)
                .append("TradeMessage.trade.product.barrierFeature.barrierDownRate", 1)
                .append("TradeMessage.trade.product.barrierFeature.knockType", 1)
                .append("TradeMessage.trade.tradeHeader.sourceSystemProductId.sourceInstrumentCategory", 1)
                .append("TradeMessage.trade.parties.executingBroker.partyName", 1)
                .append("TradeMessage.trade.tradeHeader.tradeIdentifiers.originatingTradeId.id", 1)
                .append("TradeMessage.trade.parties.counterparty.partyLei", 1)
                .append("TradeMessage.trade.tradeHeader.tradeIdentifiers.uniqueTransactionId", 1)
                .append("_id", 0);


        // Consulta para obtener los documentos con proyección
        List<Document> results2 = collection1.find(new Document()).projection(projection1).into(new ArrayList<>());
        List<Document> results1 = collection2.find(new Document()).projection(projection2).into(new ArrayList<>());

        Map<String, List<Document>> data = new HashMap<>();
        data.put("RealTime",results1);
        data.put("Sensitivities",results1);
        // Set the formatted results in the body of the exchange
        exchange.getIn().setBody(data);

    }
}
