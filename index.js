require("dotenv").config();
const mqtt = require("mqtt");
const { createClient } = require("@supabase/supabase-js");

// Variables de entorno y configuración
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const MQTT_HOST = process.env.MQTT_HOST || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const MQTT_TOPIC = process.env.MQTT_TOPIC || "application/#";

// Definición de constantes para los nombres de las tablas y otras variables
const DEVICES_TABLE = "devices";
const VOLTAGE_READINGS_TABLE = "voltage_readings";
const READINGS_TABLE = "readings";
const SENSORS_TABLE = "sensors";

// Inicializamos el cliente de Supabase
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Funciones auxiliares para manejar dispositivos
async function handleDevice(deviceId, stationId) {
  const { data: existingDevice, error: deviceSelectError } = await supabase
    .from(DEVICES_TABLE)
    .select("*")
    .eq("id", deviceId)
    .maybeSingle();

  if (deviceSelectError) {
    console.error("Error al consultar el dispositivo:", deviceSelectError);
    return;
  }

  if (!existingDevice) {
    const { data: insertedDevice, error: deviceInsertError } = await supabase
      .from(DEVICES_TABLE)
      .insert([{ id: deviceId, station_id: stationId, isActive: true }]);
    
    if (deviceInsertError) {
      console.error("Error al insertar el dispositivo:", deviceInsertError);
    } else {
      console.log("Dispositivo insertado:", insertedDevice);
    }
  } else {
    console.log("El dispositivo ya existe:", existingDevice);
  }
}

// Función para manejar lecturas de voltaje
async function handleVoltageReading(deviceId, voltage, timestamp) {
  const { data: voltageReadingData, error: voltageReadingError } = await supabase
    .from(VOLTAGE_READINGS_TABLE)
    .insert([{ device_id: deviceId, voltage, timestamp }]);
  
  if (voltageReadingError) {
    console.error("Error al insertar voltage reading:", voltageReadingError);
  } else {
    console.log("Voltage reading insertado:", voltageReadingData);
  }
}

// Función para manejar sensores y sus lecturas
async function handleSensor(sensor, stationId, timestamp) {
  if (!sensor.id) return;

  // Verificar si el sensor existe
  const { data: existingSensor, error: sensorSelectError } = await supabase
    .from(SENSORS_TABLE)
    .select("*")
    .eq("id", sensor.id)
    .maybeSingle();

  if (sensorSelectError) {
    console.error(`Error al consultar el sensor ${sensor.id}:`, sensorSelectError);
    return;
  }

  // Crear sensor si no existe
  if (!existingSensor) {
    const { error: sensorInsertError } = await supabase
      .from(SENSORS_TABLE)
      .insert([{ 
        id: sensor.id, 
        name: "", 
        sensor_type_id: sensor.t, 
        id_active: true, 
        station: stationId 
      }]);
    
    if (sensorInsertError) {
      console.error(`Error al insertar el sensor ${sensor.id}:`, sensorInsertError);
      return;
    }
  }

  // Insertar lectura del sensor
  const { error: sensorReadingError } = await supabase
    .from(READINGS_TABLE)
    .insert([{ sensor_id: sensor.id, value: sensor.v, timestamp }]);
  
  if (sensorReadingError) {
    console.error(`Error al insertar lectura para el sensor ${sensor.id}:`, sensorReadingError);
  }
}

// Función para procesar el mensaje MQTT
async function processMQTTMessage(topic, message) {
  try {
    const payloadStr = message.toString();
    const dataJson = JSON.parse(payloadStr);
    
    // Decodificación de datos Base64
    if (!dataJson.data) {
      console.error("No se encontró el campo 'data' en el mensaje.");
      return;
    }

    const decodedDataStr = Buffer.from(dataJson.data, "base64").toString("utf8");
    const decodedData = JSON.parse(decodedDataStr);
    dataJson.decodedPayload = decodedData;

    // Extraer variables importantes
    const { d: deviceId, st: stationId, ts, s: sensors, vt: voltage } = decodedData;
    const timestampISO = ts ? new Date(ts * 1000).toISOString() : new Date().toISOString();

    // Procesar dispositivo
    if (deviceId) {
      await handleDevice(deviceId, stationId);
    }

    // Procesar lectura de voltaje
    if (deviceId && voltage !== undefined && voltage !== null) {
      await handleVoltageReading(deviceId, voltage, timestampISO);
    }

    // Procesar sensores
    if (Array.isArray(sensors)) {
      for (const sensor of sensors) {
        await handleSensor(sensor, stationId, timestampISO);
      }
    }

  } catch (err) {
    console.error("Error al procesar mensaje:", err);
  }
}

// Función principal modificada
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
  
  client.on("message", processMQTTMessage);
  
  client.on("error", (err) => {
    console.error("Error en la conexión MQTT:", err);
  });
}

// Llamamos a la función principal
main();
