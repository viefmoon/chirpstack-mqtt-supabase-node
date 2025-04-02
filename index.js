require("dotenv").config();
const mqtt = require("mqtt");
const { createClient } = require("@supabase/supabase-js");

// --- Configuración y Constantes (sin cambios) ---
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MQTT_HOST = process.env.MQTT_HOST || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const MQTT_TOPIC = process.env.MQTT_TOPIC || "application/#";

const DEVICES_TABLE = "devices";
const VOLTAGE_READINGS_TABLE = "voltage_readings";
const READINGS_TABLE = "readings";
const SENSORS_TABLE = "sensors";
const STATIONS_TABLE = "stations"; // Añadir constante para tabla stations
const SENSOR_TYPES_TABLE = "sensor_types"; // Añadir constante para tabla sensor_types

const BATCH_SIZE = 100; // Número máximo de registros a insertar a la vez
const BATCH_INTERVAL = 5000; // Intervalo de tiempo para procesar el lote (ms)

// Mapeo de nombres de variables a sus IDs en la base de datos
// Estos son los identificadores que se usan para insertar en la tabla SENSOR_TYPES_TABLE
const SENSOR_TYPE_IDS = {
  TEMPERATURA: "TEMP", // ID para temperatura
  HUMEDAD: "HUM", // ID para humedad
  PH: "PH", // ID para pH
  COND: "COND", // ID para conductividad
  SOILH: "SOILH", // ID para humedad del suelo
  CO2: "CO2", // ID para CO2
  LUX: "LUX", // ID para iluminación
  PRESION: "PRES", // ID para presión
  GAS: "GAS", // ID para resistencia del gas (KOhms)
};

// Mapeo completo: cada ENUM (valor numérico) mapea a su modelo y tipos de sensores
// Estructura unificada para todos los sensores, sean de valor único o múltiple
const SENSOR_CONFIG = {
  // --- Sensores de valor único ---
  0: {
    model: "N100K", // Modelo del sensor
    types: [
      // Tipos de variables que mide
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "", // Vacío para sensores de valor único
        index: 0, // Siempre 0 para sensores de valor único
      },
    ],
  },
  1: {
    model: "N10K",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  2: {
    model: "HDS10",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  3: {
    model: "RTD",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  4: {
    model: "DS18B20",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  5: {
    model: "PH",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.PH,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  6: {
    model: "COND",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.COND,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  7: {
    model: "SOILH",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.SOILH,
        id_suffix: "",
        index: 0,
      },
    ],
  },
  8: {
    model: "VEML7700",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.LUX,
        id_suffix: "",
        index: 0,
      },
    ],
  },

  // --- Sensores múltiples ---
  100: {
    model: "SHT30",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 1,
      },
    ],
  },
  101: {
    model: "BME680",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 1,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.PRESION,
        id_suffix: "_P",
        index: 2,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.GAS,
        id_suffix: "_G",
        index: 3,
      },
    ],
  },
  102: {
    model: "CO2",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.CO2,
        id_suffix: "_CO2",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 1,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 2,
      },
    ],
  },
  103: {
    model: "BME280",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 1,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.PRESION,
        id_suffix: "_P",
        index: 2,
      },
    ],
  },
  104: {
    model: "SHT40",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 1,
      },
    ],
  },
  110: {
    model: "ENV4",
    types: [
      {
        sensor_type_id: SENSOR_TYPE_IDS.HUMEDAD,
        id_suffix: "_H",
        index: 0,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.TEMPERATURA,
        id_suffix: "_T",
        index: 1,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.PRESION,
        id_suffix: "_P",
        index: 2,
      },
      {
        sensor_type_id: SENSOR_TYPE_IDS.LUX,
        id_suffix: "_L",
        index: 3,
      },
    ],
  },
};

// --- Inicialización Supabase (sin cambios) ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// --- OPTIMIZACIÓN: Cachés en Memoria y Batch Processing ---
const knownStations = new Set();
const knownDevices = new Set();
const knownSensorTypes = new Set();
const knownSensors = new Set();

// Colas para procesamiento por lotes
const readingsBatch = [];
const voltageReadingsBatch = [];

// --- PRECARGA de datos existentes ---
async function preloadExistingData() {
  console.log("Iniciando precarga de datos existentes...");

  try {
    // Cargar estaciones
    const { data: stations, error: stationsError } = await supabase
      .from(STATIONS_TABLE)
      .select("id");

    if (stationsError) throw stationsError;
    stations.forEach((station) => knownStations.add(station.id));
    console.log(`Precargadas ${stations.length} estaciones`);

    // Cargar dispositivos
    const { data: devices, error: devicesError } = await supabase
      .from(DEVICES_TABLE)
      .select("id");

    if (devicesError) throw devicesError;
    devices.forEach((device) => knownDevices.add(device.id));
    console.log(`Precargados ${devices.length} dispositivos`);

    // Cargar tipos de sensores
    const { data: sensorTypes, error: sensorTypesError } = await supabase
      .from(SENSOR_TYPES_TABLE)
      .select("id");

    if (sensorTypesError) throw sensorTypesError;
    sensorTypes.forEach((type) => knownSensorTypes.add(type.id));
    console.log(`Precargados ${sensorTypes.length} tipos de sensores`);

    // Cargar sensores (solo IDs para la caché)
    const { data: sensors, error: sensorsError } = await supabase
      .from(SENSORS_TABLE)
      .select("id");

    if (sensorsError) throw sensorsError;
    sensors.forEach((sensor) => knownSensors.add(sensor.id));
    console.log(`Precargados ${sensors.length} sensores`);

    console.log("Precarga de datos completada con éxito");
  } catch (error) {
    console.error("Error durante la precarga de datos:", error);
  }
}

// Procesamiento por lotes para lecturas
async function processBatches() {
  try {
    // Procesar lecturas de sensores
    if (readingsBatch.length > 0) {
      const batchToProcess = [...readingsBatch];
      readingsBatch.length = 0; // Limpiar la cola original

      const { error } = await supabase
        .from(READINGS_TABLE)
        .insert(batchToProcess);

      if (error) {
        console.error("Error al insertar lote de lecturas:", error);
        // En caso de error, podríamos intentar reinsertar o procesar uno por uno
      } else {
        console.log(`Procesado lote de ${batchToProcess.length} lecturas`);
      }
    }

    // Procesar lecturas de voltaje
    if (voltageReadingsBatch.length > 0) {
      const batchToProcess = [...voltageReadingsBatch];
      voltageReadingsBatch.length = 0; // Limpiar la cola original

      const { error } = await supabase
        .from(VOLTAGE_READINGS_TABLE)
        .insert(batchToProcess);

      if (error) {
        console.error("Error al insertar lote de lecturas de voltaje:", error);
      } else {
        console.log(
          `Procesado lote de ${batchToProcess.length} lecturas de voltaje`
        );
      }
    }
  } catch (err) {
    console.error("Error en procesamiento por lotes:", err);
  }
}

// --- Funciones Auxiliares Optimizadas ---

async function ensureStationExists(stationId) {
  if (knownStations.has(stationId)) {
    return true; // Ya verificado en esta sesión
  }

  // Usar upsert: inserta si no existe, ignora si ya existe (basado en el PK 'id')
  const { error } = await supabase
    .from(STATIONS_TABLE)
    .upsert(
      { id: stationId, name: `Estación ${stationId}`, is_active: true },
      { onConflict: "id", ignoreDuplicates: true }
    ); // Ajusta 'id' si tu PK tiene otro nombre

  if (error) {
    console.error(
      `Error al asegurar/insertar la estación ${stationId}:`,
      error
    );
    return false; // Hubo un error (no necesariamente que ya existía)
  }

  console.log(`Estación ${stationId} asegurada o ya existía.`);
  knownStations.add(stationId); // Añadir al caché después del éxito
  return true;
}

async function ensureSensorTypeExists(sensorTypeId) {
  // Verificar si el tipo de sensor es válido
  if (!sensorTypeId) {
    console.error(`Tipo de sensor no válido: ${sensorTypeId}`);
    return null; // Devolver null para indicar fallo
  }

  if (knownSensorTypes.has(sensorTypeId)) {
    return sensorTypeId; // Ya verificado en esta sesión
  }

  // Usar upsert
  const { error } = await supabase
    .from(SENSOR_TYPES_TABLE)
    .upsert(
      { id: sensorTypeId, name: sensorTypeId },
      { onConflict: "id", ignoreDuplicates: true }
    );

  if (error) {
    console.error(
      `Error al asegurar/insertar tipo de sensor ${sensorTypeId}:`,
      error
    );
    return null; // Hubo un error
  }

  console.log(`Tipo de sensor ${sensorTypeId} asegurado o ya existía.`);
  knownSensorTypes.add(sensorTypeId); // Añadir al caché
  return sensorTypeId;
}

async function ensureDeviceExists(deviceId, stationId) {
  // Asegurar primero la estación (usa la función optimizada)
  const stationOk = await ensureStationExists(stationId);
  if (!stationOk) {
    console.error(
      `No se pudo asegurar la estación ${stationId}, abortando para el dispositivo ${deviceId}`
    );
    return false;
  }

  if (knownDevices.has(deviceId)) {
    return true; // Ya verificado en esta sesión
  }

  // Usar upsert
  const { error } = await supabase
    .from(DEVICES_TABLE)
    .upsert(
      { id: deviceId, station_id: stationId, is_active: true },
      { onConflict: "id", ignoreDuplicates: true }
    ); // Ajusta 'id' si tu PK tiene otro nombre

  if (error) {
    console.error(
      `Error al asegurar/insertar el dispositivo ${deviceId}:`,
      error
    );
    return false; // Hubo un error
  }

  console.log(`Dispositivo ${deviceId} asegurado o ya existía.`);
  knownDevices.add(deviceId); // Añadir al caché
  return true;
}

async function ensureSensorExists(sensorId, sensorTypeId, stationId) {
  if (knownSensors.has(sensorId)) {
    return true; // Ya verificado en esta sesión
  }

  // Upsert para el sensor
  const { error } = await supabase.from(SENSORS_TABLE).upsert(
    {
      id: sensorId,
      name: "", // Opcional: podrías intentar darle un nombre por defecto más descriptivo
      sensor_type_id: sensorTypeId,
      is_active: true,
      station_id: stationId,
    },
    { onConflict: "id", ignoreDuplicates: true } // Ajusta 'id' si tu PK tiene otro nombre
  );

  if (error) {
    console.error(`Error al asegurar/insertar el sensor ${sensorId}:`, error);
    return false; // Hubo un error
  }

  console.log(`Sensor ${sensorId} asegurado o ya existía.`);
  knownSensors.add(sensorId); // Añadir al caché
  return true;
}

// Función para manejar lecturas de voltaje (optimizada con batch)
function handleVoltageReading(deviceId, voltage, timestamp) {
  voltageReadingsBatch.push({
    device_id: deviceId,
    voltage_value: voltage,
    timestamp,
  });

  // Si alcanzamos el tamaño del lote, procesar inmediatamente
  if (voltageReadingsBatch.length >= BATCH_SIZE) {
    processBatches();
  }
}

// Función para manejar una lectura de sensor individual (ahora usa batching)
async function handleSensorReading(
  sensorId,
  sensorTypeId,
  value,
  stationId,
  timestamp
) {
  if (value === null || value === undefined) {
    // console.log(`Omitiendo lectura nula/undefined para sensor ${sensorId}`);
    return; // No procesar valores nulos
  }
  if (!sensorId) {
    console.error("Intento de procesar lectura de sensor sin ID válido.");
    return;
  }

  // 1. Asegurar que el tipo de sensor existe (usa caché + upsert)
  const confirmedSensorTypeId = await ensureSensorTypeExists(sensorTypeId);
  if (!confirmedSensorTypeId) {
    console.error(
      `No se pudo asegurar el tipo de sensor ${sensorTypeId} para el sensor ${sensorId}. Abortando lectura.`
    );
    return;
  }

  // 2. Asegurar que el sensor existe (usa caché + upsert)
  const sensorOk = await ensureSensorExists(
    sensorId,
    confirmedSensorTypeId,
    stationId
  );
  if (!sensorOk) {
    console.error(
      `No se pudo asegurar el sensor ${sensorId}. Abortando lectura.`
    );
    return; // Importante: No insertar lectura si el sensor no pudo ser asegurado/creado
  }

  // 3. Añadir la lectura al lote en lugar de insertar directamente
  readingsBatch.push({
    sensor_id: sensorId,
    value: value,
    timestamp,
  });

  // Si alcanzamos el tamaño del lote, procesar inmediatamente
  if (readingsBatch.length >= BATCH_SIZE) {
    processBatches();
  }
}

// --- Función para procesar el mensaje MQTT (Modificada para usar nuevas funciones) ---
async function processMQTTMessage(topic, message) {
  try {
    const payloadStr = message.toString();
    // console.log("Mensaje MQTT recibido:", { topic, payload: payloadStr });

    const messageJson = JSON.parse(payloadStr);
    const decodedData = Buffer.from(messageJson.data, "base64").toString(
      "utf8"
    );
    // console.log("Datos decodificados:", decodedData);

    const parts = decodedData.split("|");
    if (parts.length < 4) {
      console.error("Formato de mensaje decodificado inválido:", decodedData);
      return;
    }

    const [stationId, deviceId, voltageStr, timestampStr, ...sensorData] =
      parts;

    // Validar y convertir timestamp
    const timestampNum = parseInt(timestampStr);
    if (isNaN(timestampNum)) {
      console.error("Error: Timestamp inválido:", timestampStr);
      return;
    }
    const timestampISO = new Date(timestampNum * 1000).toISOString();

    // 1. Asegurar Dispositivo (esto a su vez asegura la estación)
    const deviceOk = await ensureDeviceExists(deviceId, stationId);
    if (!deviceOk) {
      console.error(
        `No se pudo asegurar el dispositivo ${deviceId}, omitiendo procesamiento de lecturas para este mensaje.`
      );
      return; // Si el dispositivo falla, no tiene sentido seguir
    }

    // 2. Procesar Lectura de Voltaje
    const voltage = parseFloat(voltageStr);
    if (!isNaN(voltage)) {
      handleVoltageReading(deviceId, voltage, timestampISO);
    } else {
      // console.log(`Valor de voltaje no válido para ${deviceId}: ${voltageStr}`);
    }

    // 3. Procesar Sensores
    for (const sensorStr of sensorData) {
      const sensorParts = sensorStr.split(",");
      if (sensorParts.length < 3) {
        console.warn(`Formato de sensor inválido, omitiendo: "${sensorStr}"`);
        continue;
      }

      const sensorId = sensorParts[0];
      const sensorModelEnum = parseInt(sensorParts[1]);

      if (isNaN(sensorModelEnum)) {
        console.warn(
          `Tipo de sensor inválido para ${sensorId}: "${sensorParts[1]}"`
        );
        continue;
      }

      // Verificar si el tipo de sensor existe en la configuración
      if (!SENSOR_CONFIG[sensorModelEnum]) {
        console.warn(
          `Sensor no configurado: ${sensorId} tipo ${sensorModelEnum}`
        );
        continue;
      }

      const sensorConfig = SENSOR_CONFIG[sensorModelEnum];
      console.log(
        `Procesando sensor: ${sensorId} modelo ${sensorConfig.model}`
      );

      // Manejar sensores (múltiples o de valor único usando la misma lógica)
      for (const typeConfig of sensorConfig.types) {
        const valueIndex = typeConfig.index + 2; // id, tipo, valor0, valor1...

        // Verificar que tengamos suficientes valores en el mensaje
        if (sensorParts.length <= valueIndex) {
          console.warn(
            `No hay suficientes valores para el sensor ${sensorId} (índice ${valueIndex})`
          );
          continue;
        }

        const rawValue = sensorParts[valueIndex];
        const value =
          rawValue.toLowerCase() === "nan" ? null : parseFloat(rawValue);

        // Para sensores de valor único, no usar sufijo
        const derivedSensorId = typeConfig.id_suffix
          ? `${sensorId}${typeConfig.id_suffix}`
          : sensorId;

        if (value === null || value === undefined) {
          console.log(`Omitiendo valor nulo para ${derivedSensorId}`);
          continue;
        }

        console.log(
          `Procesando lectura para ${derivedSensorId} tipo ${typeConfig.sensor_type_id}`
        );

        // Usar la función de manejo de lectura individual
        await handleSensorReading(
          derivedSensorId,
          typeConfig.sensor_type_id,
          value,
          stationId,
          timestampISO
        );
      }
    }

    console.log(
      `Mensaje procesado para Estación: ${stationId}, Dispositivo: ${deviceId}`
    );
  } catch (err) {
    console.error("Error fatal al procesar mensaje MQTT:", {
      errorMessage: err.message,
      errorStack: err.stack,
      topic: topic,
      message: message.toString(), // Loguear el mensaje original en caso de error
    });
  }
}

// --- Función Principal (modificada para incluir precarga y procesamiento por lotes) ---
async function main() {
  // Precargar datos existentes para optimizar
  await preloadExistingData();

  // Iniciar el procesamiento por lotes periódico
  setInterval(processBatches, BATCH_INTERVAL);

  const brokerUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;
  const client = mqtt.connect(brokerUrl);

  client.on("connect", () => {
    console.log("Conectado al broker MQTT con éxito.");
    client.subscribe(MQTT_TOPIC, (err) => {
      if (!err) {
        console.log(`Suscrito al topic: ${MQTT_TOPIC}`);
      } else {
        console.error("Error al suscribirse al topic MQTT:", err);
      }
    });
  });

  client.on("message", (topic, message) => {
    processMQTTMessage(topic, message);
  });

  client.on("error", (err) => {
    console.error("Error en la conexión MQTT:", err);
  });

  // Manejar cierre limpio
  process.on("SIGINT", async () => {
    console.log("Cerrando aplicación, procesando lotes pendientes...");
    await processBatches();
    console.log("Procesamiento finalizado. Saliendo.");
    process.exit(0);
  });

  console.log(`Intentando conectar a ${brokerUrl}...`);
}

// --- Llamada a la función principal ---
main();
