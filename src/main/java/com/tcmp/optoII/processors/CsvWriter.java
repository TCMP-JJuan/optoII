package com.tcmp.optoII.processors;


import com.opencsv.CSVWriter;
import com.tcmp.optoII.model.OptosIIRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Component;

import java.io.CharArrayWriter;
import java.util.List;

@Component
@Slf4j
public class CsvWriter {

    public void writeToCsv(Exchange exchange) {
        // Obtener la lista de TradeRecord desde el Exchange
        List<OptosIIRecord> optosIIRecords = exchange.getIn().getBody(List.class);

        log.info("Datos recibidos en writeToCsv: {}", optosIIRecords.toString());

        if (optosIIRecords == null || optosIIRecords.isEmpty()) {
            // Si la lista está vacía, no hacemos nada
            return;
        }

        // Usar CharArrayWriter para generar el CSV en memoria
        try (CharArrayWriter writer = new CharArrayWriter()) {
            CSVWriter csvWriter = new CSVWriter(writer, '|', CSVWriter.NO_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);

            // Escribir los encabezados del CSV
            String[] header = {
                    "INST", "OFICINA", "LEI", "CONTRAPAR", "FE_CON_OPE", "FE_INI_OPE", "FE_VEN_OPE", "OTRO_DER", "DIASLIQ",
                    "POSICION", "TIP_OPC", "TIP_OPC2", "OPC_EJER", "OBJETIVO", "IMPBASE", "MDAIMP", "LIQUIDA", "MDALIQUIDA",
                    "FE_FLU", "FEN_FLU", "DET_FLUJO", "NU_FLUJO", "INT_FLUJO", "TASA_REF", "REV_TREF", "ANIO", "FE_REF",
                    "FAC_TASA", "SPREAD", "TASA_FIJA", "CAL_LIQ", "CAL_LIQ2", "FAC_LIQ", "DIF", "FAC_LIQ2", "DIF2",
                    "MMRIA_PAGO", "CON_TERM", "N_CAN", "QUANTO", "TC_QUANT", "PRIMA", "MDAPRIMA", "FE_PRIM", "PAQ_EST",
                    "ID_PAQ_EST", "CON_PAQ_EST", "TIP_DER", "SUBY", "CVE_TIT", "INT_EJE", "INT_MON", "N_MONSUBY", "NU_TO_EJE",
                    "NUM_ID_OP_SBY", "SECCION_SWP", "NUMSUBY", "MDASUBY", "PRECIOEJER", "TIP_PRECIO", "MDAPRECIO",
                    "CAL_EJERCICIO", "N_EJERCICIO", "PRECIOEJER2", "TIP_PRECIO2", "MDAPRECIO2", "CAL_EJERCICIO2",
                    "N_EJERCICIO2", "PRECIO_INICIAL", "PRE_SUP", "PRE_INF", "MOD_PRE", "INT_MON_BA", "FE_INI_VEN",
                    "FE_VEN_VEN", "PRE_SUP2", "PRE_INF2", "MOD_PRE2", "INT_MON_BA2", "FE_INI_VEN2", "FE_VEN_VEN2", "REBATE",
                    "CALLABLE", "REV_OP", "BROKER", "SOCIO_LIQ", "CAM_COM", "REP_DEV", "AG_CAL", "UPI", "DELTA", "VOL",
                    "ID_CONT_ANF", "NUM_ID_INST", "NUM_ID", "UTI", "IDENTIFICADOR"
            };


            csvWriter.writeNext(header);

            // Escribir cada TradeRecord como una nueva línea en el archivo CSV
            for (OptosIIRecord record : optosIIRecords) {
                String[] data = {
                        record.getInst(),            // INST
                        record.getOficina(),         // OFICINA
                        record.getContrapar(),       // CONTRAPAR
                        record.getFeConOpe(),        // FE_CON_OPE
                        record.getFeIniOpe(),        // FE_INI_OPE
                        record.getFeVenOpe(),        // FE_VEN_OPE
                        String.valueOf(record.getDiasLiq()),         // DIASLIQ
                        record.getPosicion(),        // POSICION
                        record.getTipOpc(),          // TIP_OPC
                        record.getOpcLiq(),          // OPC_LIQ
                        record.getObjetivo(),        // OBJETIVO
                        record.getImpBase(),         // IMPBASE
                        record.getMdaImp(),          // MDAIMP
                        record.getLiquida(),         // LIQUIDA
                        record.getMdaLiquida(),      // MDALIQUIDA
                        record.getQuanto(),          // QUANTO
                        record.getTcQuant(),         // TC_QUANT
                        record.getPrima(),           // PRIMA
                        record.getMdaPrima(),        // MDAPRIMA
                        record.getFePrim(),          // FE_PRIM
                        record.getPaqEst(),          // PAQ_EST
                        record.getIdPaqEst(),        // ID_PAQ_EST
                        record.getConPaqEst(),       // CON_PAQ_EST
                        record.getSuby(),            // SUBY
                        record.getCveTit(),          // CVE_TIT
                        record.getIntEje(),          // INT_EJE
                        record.getIntMon(),          // INT_MON
                        record.getNuToEje(),         // NU_TO_EJE
                        record.getNumIdOpSby(),      // NUM_ID_OP_SBY
                        record.getNumSuby(),         // NUMSUBY
                        record.getMdaSuby(),         // MDASUBY
                        record.getPrecioEjer(),      // PRECIOEJER
                        record.getMdaPrecio(),       // MDAPRECIO
                        record.getPreSup(),          // PRE_SUP
                        record.getPreInf(),          // PRE_INF
                        record.getModPre(),          // MOD_PRE
                        record.getTipDer(),          // TIP_DER
                        record.getRevOp(),           // REV_OP
                        record.getBroker(),          // BROKER
                        record.getSocioLiq(),        // SOCIO_LIQ
                        record.getCamCom(),          // CAM_COM
                        record.getAgCal(),           // AG_CAL
                        record.getNumConf(),         // NUM_CONF
                        record.getDelta(),           // DELTA
                        record.getNumId(),           // NUM_ID
                        record.getInstLei(),         // INST_LEI
                        record.getUti(),             // UTI
                        record.getUpi(),             // UPI
                        record.getIdentificador()    // IDENTIFICADOR
                };

                csvWriter.writeNext(data);
            }

            // Agregar el CSV generado al Exchange para enviarlo como respuesta
            exchange.getMessage().setBody(writer.toString());
            exchange.getMessage().setHeader("Content-Type", "text/csv");
            exchange.getMessage().setHeader("Content-Disposition", "attachment; filename=trade_records.csv");

        }
    }
}
