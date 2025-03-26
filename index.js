require("dotenv").config();
const mqtt = require("mqtt");
const { createClient } = require("@supabase/supabase-js");

// Variables de entorno y configuración
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MQTT_HOST = process.env.MQTT_HOST || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const MQTT_TOPIC = process.env.MQTT_TOPIC || "application/#";

// Definición de constantes para los nombres de las tablas y otras variables
const DEVICES_TABLE = "devices";
const VOLTAGE_READINGS_TABLE = "voltage_readings";
const READINGS_TABLE = "readings";
const SENSORS_TABLE = "sensors";

// Agregar el mapeo de enum a ID de tipo de sensor
const SENSOR_TYPE_ENUM_MAP = {
  0: "N100K", // NTC 100K
  1: "N10K", // NTC 10K
  2: "HDS10", // Condensation Humidity
  3: "RTD", // RTD
  4: "DS18B20", // DS18B20
  5: "PH", // PH
  6: "COND", // Conductivity
  7: "CONDH", // Condensation Humidity
  8: "SOILH", // Soil Humidity
  9: "TEMP_A", // Temperatura ambiente
  10: "HUM_A", // Humedad ambiente
  11: "PRESS_A", // Presión atmosférica
  12: "CO2", // Dióxido de Carbono
  13: "LIGHT", // Luz Ambiental
  14: "ROOTH", // Humedad de Raíz
  15: "LEAFH", // Humedad de Hoja
  // SENSORES MULTIPLES
  100: "SHT30", // Sensor SHT30 (tipo general para el sensor completo - SENSOR MÚLTIPLE)
};

// Mapa para los sensores que envían múltiples valores
const MULTI_SENSOR_MAP = {
  100: [
    // SHT30
    { id_suffix: "_T", typeEnum: 9, index: 0 }, // Temperatura
    { id_suffix: "_H", typeEnum: 10, index: 1 }, // Humedad
  ],
};

// Inicializamos el cliente de Supabase
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Funciones auxiliares para manejar dispositivos
async function handleStation(stationId) {
  const { data: existingStation, error: stationSelectError } = await supabase
    .from("stations")
    .select("*")
    .eq("id", stationId)
    .maybeSingle();

  if (stationSelectError) {
    console.error("Error al consultar la estación:", stationSelectError);
    return false;
  }

  if (!existingStation) {
    const { error: stationInsertError } = await supabase
      .from("stations")
      .insert([
        { id: stationId, name: `Estación ${stationId}`, is_active: true },
      ]);

    if (stationInsertError) {
      console.error("Error al insertar la estación:", stationInsertError);
      return false;
    }
  }
  return true;
}

// Modificar la función handleSensorType para usar el mapeo
async function handleSensorType(sensorTypeEnum) {
  const sensorTypeId = SENSOR_TYPE_ENUM_MAP[sensorTypeEnum];

  if (!sensorTypeId) {
    console.error(`Tipo de sensor no válido: ${sensorTypeEnum}`);
    return false;
  }

  const { data: existingSensorType, error: sensorTypeSelectError } =
    await supabase
      .from("sensor_types")
      .select("*")
      .eq("id", sensorTypeId)
      .maybeSingle();

  if (sensorTypeSelectError) {
    console.error(
      "Error al consultar el tipo de sensor:",
      sensorTypeSelectError
    );
    return false;
  }

  if (!existingSensorType) {
    const { error: sensorTypeInsertError } = await supabase
      .from("sensor_types")
      .insert([{ id: sensorTypeId, name: sensorTypeId }]);

    if (sensorTypeInsertError) {
      console.error(
        "Error al insertar el tipo de sensor:",
        sensorTypeInsertError
      );
      return false;
    }
  }
  return sensorTypeId;
}

// Modificar la función handleDevice para usar handleStation
async function handleDevice(deviceId, stationId) {
  // Primero verificar y crear la estación si es necesario
  const stationExists = await handleStation(stationId);
  if (!stationExists) return;

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
      .insert([{ id: deviceId, station_id: stationId, is_active: true }]);

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
  const { data: voltageReadingData, error: voltageReadingError } =
    await supabase
      .from(VOLTAGE_READINGS_TABLE)
      .insert([{ device_id: deviceId, voltage_value: voltage, timestamp }])
      .select();

  if (voltageReadingError) {
    console.error("Error al insertar voltage reading:", voltageReadingError);
  } else {
    console.log("Voltage reading insertado:", voltageReadingData[0]);
  }
}

// Modificar la función handleSensor para manejar valores nulos
async function handleSensor(sensor, stationId, timestamp) {
  if (!sensor.id) return;

  // Verificar y crear el tipo de sensor si es necesario
  const sensorTypeId = await handleSensorType(sensor.t);
  if (!sensorTypeId) return;

  // Si el valor es null, no insertar la lectura
  if (sensor.v === null) {
    console.log(`Omitiendo lectura nula para sensor ${sensor.id}`);
    return;
  }

  // Verificar si el sensor existe
  const { data: existingSensor, error: sensorSelectError } = await supabase
    .from(SENSORS_TABLE)
    .select("*")
    .eq("id", sensor.id)
    .maybeSingle();

  if (sensorSelectError) {
    console.error(
      `Error al consultar el sensor ${sensor.id}:`,
      sensorSelectError
    );
    return;
  }

  if (!existingSensor) {
    const { error: sensorInsertError } = await supabase
      .from(SENSORS_TABLE)
      .insert([
        {
          id: sensor.id,
          name: "",
          sensor_type_id: sensorTypeId,
          is_active: true,
          station_id: stationId,
        },
      ]);

    if (sensorInsertError) {
      console.error(
        `Error al insertar el sensor ${sensor.id}:`,
        sensorInsertError
      );
      return;
    }
  }

  // Insertar lectura del sensor solo si el valor no es nulo
  const { error: sensorReadingError } = await supabase
    .from(READINGS_TABLE)
    .insert([
      {
        sensor_id: sensor.id,
        value: sensor.v,
        timestamp,
      },
    ]);

  if (sensorReadingError) {
    console.error(
      `Error al insertar lectura para el sensor ${sensor.id}:`,
      sensorReadingError
    );
  }
}

// Función para procesar el mensaje MQTT
async function processMQTTMessage(topic, message) {
  try {
    const payloadStr = message.toString();
    console.log("Mensaje MQTT recibido:", {
      topic,
      payload: payloadStr,
    });

    // Primero parseamos el JSON del mensaje
    const messageJson = JSON.parse(payloadStr);

    // Decodificamos el campo data que está en Base64
    const decodedData = Buffer.from(messageJson.data, "base64").toString(
      "utf8"
    );
    console.log("Datos decodificados:", decodedData);

    // Ahora procesamos los datos decodificados
    const parts = decodedData.split("|");
    console.log("Partes del mensaje:", parts);

    const [stationId, deviceId, voltage, timestamp, ...sensorData] = parts;

    console.log("Valores extraídos:", {
      stationId,
      deviceId,
      voltage,
      timestamp: timestamp,
      timestampNumber: parseInt(timestamp),
      sensorData,
    });

    // Validar timestamp antes de convertirlo
    const timestampNum = parseInt(timestamp);
    if (isNaN(timestampNum)) {
      console.error("Error: El timestamp no es un número válido:", timestamp);
      return;
    }

    // Convertir el timestamp a formato ISO
    try {
      const timestampISO = new Date(timestampNum * 1000).toISOString();
      console.log("Timestamp convertido:", {
        original: timestamp,
        parsed: timestampNum,
        multiplied: timestampNum * 1000,
        iso: timestampISO,
      });

      console.log("Datos recibidos:", {
        stationId,
        deviceId,
        voltage,
        timestamp: timestampISO,
      });

      // Procesar dispositivo
      if (deviceId && stationId) {
        await handleDevice(deviceId, stationId);
      }

      // Procesar lectura de voltaje
      if (deviceId && voltage !== undefined && voltage !== null) {
        await handleVoltageReading(deviceId, parseFloat(voltage), timestampISO);
      }

      // Procesar sensores
      for (const sensorStr of sensorData) {
        console.log("Procesando sensor:", sensorStr);

        // Dividir los datos del sensor por comas
        const sensorParts = sensorStr.split(",");
        const sensorId = sensorParts[0];
        const sensorType = parseInt(sensorParts[1]);

        // Verificar si es un sensor multi-valor como SHT30
        if (MULTI_SENSOR_MAP[sensorType] && sensorParts.length > 3) {
          console.log(
            `Procesando sensor multivalor: ${sensorId} de tipo ${sensorType}`
          );

          // Procesar cada valor según la configuración del mapa
          for (const config of MULTI_SENSOR_MAP[sensorType]) {
            const valueIndex = config.index + 2; // +2 porque los primeros 2 son id y tipo

            if (sensorParts.length > valueIndex) {
              const sensorValue = sensorParts[valueIndex];
              // Convertir 'nan' a null para valores no disponibles
              const value =
                sensorValue.toLowerCase() === "nan"
                  ? null
                  : parseFloat(sensorValue);

              if (value !== null) {
                // Crear un nuevo ID con el sufijo correspondiente
                const newSensorId = `${sensorId}${config.id_suffix}`;

                console.log(
                  `Procesando subvalor: ${newSensorId} con valor ${value}`
                );

                // Crear objeto sensor con el formato esperado por handleSensor
                const sensor = {
                  id: newSensorId,
                  t: config.typeEnum,
                  v: value,
                };

                await handleSensor(sensor, stationId, timestampISO);
              } else {
                console.log(
                  `Omitiendo valor nulo para ${sensorId}${config.id_suffix}`
                );
              }
            }
          }
        } else if (!MULTI_SENSOR_MAP[sensorType]) {
          // Procesamiento normal para sensores de un solo valor
          // Solo procesar si NO es un sensor múltiple
          const sensorValue = sensorParts[2];

          // Convertir 'nan' a null para valores no disponibles
          const value =
            sensorValue.toLowerCase() === "nan"
              ? null
              : parseFloat(sensorValue);

          console.log("Datos del sensor:", {
            sensorId,
            sensorType,
            sensorValue,
            parsedValue: value,
          });

          // Crear objeto sensor con el formato esperado por handleSensor
          const sensor = {
            id: sensorId,
            t: sensorType,
            v: value,
          };

          await handleSensor(sensor, stationId, timestampISO);
        } else {
          console.log(
            `Sensor multivalor ${sensorId} tipo ${sensorType} detectado pero formato incorrecto`
          );
        }
      }
    } catch (timeError) {
      console.error("Error al procesar el timestamp:", {
        timestamp,
        error: timeError.message,
        stack: timeError.stack,
      });
    }
  } catch (err) {
    console.error("Error al procesar mensaje:", {
      error: err.message,
      stack: err.stack,
    });
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
