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

const SENSOR_TYPE_ENUM_MAP = {
  0: "N100K",
  1: "N10K",
  2: "HDS10",
  3: "RTD",
  4: "DS18B20",
  5: "PH",
  6: "COND",
  7: "CONDH",
  8: "SOILH",
  9: "TEMP_A",
  10: "HUM_A",
  11: "PRESS_A",
  12: "CO2",
  13: "LIGHT",
  14: "ROOTH",
  15: "LEAFH",
  100: "SHT30",
};

const MULTI_SENSOR_MAP = {
  100: [
    // SHT30
    { id_suffix: "_T", typeEnum: 9, index: 0 }, // Temperatura
    { id_suffix: "_H", typeEnum: 10, index: 1 }, // Humedad
  ],
};

// --- Inicialización Supabase (sin cambios) ---
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// --- OPTIMIZACIÓN: Cachés en Memoria ---
const knownStations = new Set();
const knownDevices = new Set();
const knownSensorTypes = new Set();
const knownSensors = new Set();
// -----------------------------------------

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

async function ensureSensorTypeExists(sensorTypeEnum) {
  const sensorTypeId = SENSOR_TYPE_ENUM_MAP[sensorTypeEnum];
  if (!sensorTypeId) {
    console.error(`Tipo de sensor no válido en mapeo: ${sensorTypeEnum}`);
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
    ); // Ajusta 'id' si tu PK tiene otro nombre

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

// Función para manejar lecturas de voltaje (sin cambios grandes, pero podría beneficiarse de batching)
async function handleVoltageReading(deviceId, voltage, timestamp) {
  // Considerar añadir esta inserción a un buffer para batching si el volumen es alto
  const { data: voltageReadingData, error: voltageReadingError } =
    await supabase
      .from(VOLTAGE_READINGS_TABLE)
      .insert([{ device_id: deviceId, voltage_value: voltage, timestamp }])
      .select(); // select() opcional si no necesitas el resultado

  if (voltageReadingError) {
    console.error("Error al insertar voltage reading:", voltageReadingError);
  } else {
    // console.log("Voltage reading insertado:", voltageReadingData?.[0]); // Log menos verboso
  }
}

// Función para manejar una lectura de sensor individual (ahora usa las funciones "ensure")
async function handleSensorReading(
  sensorId,
  sensorTypeEnum,
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
  const sensorTypeId = await ensureSensorTypeExists(sensorTypeEnum);
  if (!sensorTypeId) {
    console.error(
      `No se pudo asegurar el tipo de sensor ${sensorTypeEnum} para el sensor ${sensorId}. Abortando lectura.`
    );
    return;
  }

  // 2. Asegurar que el sensor existe (usa caché + upsert)
  const sensorOk = await ensureSensorExists(sensorId, sensorTypeId, stationId);
  if (!sensorOk) {
    console.error(
      `No se pudo asegurar el sensor ${sensorId}. Abortando lectura.`
    );
    return; // Importante: No insertar lectura si el sensor no pudo ser asegurado/creado
  }

  // 3. Insertar la lectura (Considerar añadir esta inserción a un buffer para batching)
  const { error: readingInsertError } = await supabase
    .from(READINGS_TABLE)
    .insert([
      {
        sensor_id: sensorId,
        value: value,
        timestamp,
      },
    ]);

  if (readingInsertError) {
    console.error(
      `Error al insertar lectura para el sensor ${sensorId}:`,
      readingInsertError
    );
  } else {
    // console.log(`Lectura insertada para ${sensorId}`); // Log menos verboso
  }
}

// --- Función para procesar el mensaje MQTT (Modificada para usar nuevas funciones) ---
async function processMQTTMessage(topic, message) {
  try {
    const payloadStr = message.toString();
    // console.log("Mensaje MQTT recibido:", { topic, payload: payloadStr }); // Menos verbosidad

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
      await handleVoltageReading(deviceId, voltage, timestampISO);
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
      const sensorType = parseInt(sensorParts[1]);

      if (isNaN(sensorType)) {
        console.warn(
          `Tipo de sensor inválido para ${sensorId}: "${sensorParts[1]}"`
        );
        continue;
      }

      // Manejo de Sensores Múltiples (como SHT30)
      if (MULTI_SENSOR_MAP[sensorType] && sensorParts.length > 2) {
        // Necesita al menos id, tipo, valor1...
        // console.log(`Procesando sensor multivalor: ${sensorId} tipo ${sensorType}`);
        for (const config of MULTI_SENSOR_MAP[sensorType]) {
          const valueIndex = config.index + 2; // id, tipo, valor0, valor1...
          if (sensorParts.length > valueIndex) {
            const rawValue = sensorParts[valueIndex];
            const value =
              rawValue.toLowerCase() === "nan" ? null : parseFloat(rawValue);
            const derivedSensorId = `${sensorId}${config.id_suffix}`;

            // Usar la función de manejo de lectura individual
            await handleSensorReading(
              derivedSensorId,
              config.typeEnum,
              value,
              stationId,
              timestampISO
            );
          }
        }
      }
      // Manejo de Sensores de Valor Único
      else if (!MULTI_SENSOR_MAP[sensorType]) {
        const rawValue = sensorParts[2];
        const value =
          rawValue.toLowerCase() === "nan" ? null : parseFloat(rawValue);

        // Usar la función de manejo de lectura individual
        await handleSensorReading(
          sensorId,
          sensorType,
          value,
          stationId,
          timestampISO
        );
      } else {
        console.warn(
          `Sensor multivalor ${sensorId} tipo ${sensorType} detectado pero formato inesperado: ${sensorStr}`
        );
      }
    }
    console.log(
      `Mensaje procesado para Estación: ${stationId}, Dispositivo: ${deviceId}`
    ); // Log al final del procesamiento
  } catch (err) {
    console.error("Error fatal al procesar mensaje MQTT:", {
      errorMessage: err.message,
      errorStack: err.stack,
      topic: topic,
      message: message.toString(), // Loguear el mensaje original en caso de error
    });
  }
}

// --- Función Principal (sin cambios) ---
async function main() {
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

  // Procesar mensajes uno por uno. Si la carga es muy alta, podrías necesitar un pool o cola.
  client.on("message", (topic, message) => {
    processMQTTMessage(topic, message); // No usar await aquí para no bloquear la recepción de nuevos mensajes
  });

  client.on("error", (err) => {
    console.error("Error en la conexión MQTT:", err);
  });

  console.log(`Intentando conectar a ${brokerUrl}...`);
}

// --- Llamada a la función principal ---
main();
