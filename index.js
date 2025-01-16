require("dotenv").config();
const mqtt = require("mqtt");
const { createClient } = require("@supabase/supabase-js");

// Cargamos variables de entorno
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const SUPABASE_TABLE = process.env.SUPABASE_TABLE || "sensor_data";
const MQTT_HOST = process.env.MQTT_HOST || "localhost";
const MQTT_PORT = process.env.MQTT_PORT || 1883;
const MQTT_TOPIC = process.env.MQTT_TOPIC || "application/#";

// Inicializamos cliente de Supabase
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Función principal
async function main() {
  // Creamos la URL del broker, p.ej. mqtt://localhost:1883
  const brokerUrl = `mqtt://${MQTT_HOST}:${MQTT_PORT}`;

  // Opcional: si tu broker MQTT requiere credenciales,
  // puedes pasarlas en las options:
  //   const options = { username: "usuario", password: "contraseña" };

  const client = mqtt.connect(brokerUrl);

  // Callback cuando se conecta
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

  // Callback al recibir mensajes
  client.on("message", async (topic, message) => {
    try {
      const payloadStr = message.toString();
      const dataJson = JSON.parse(payloadStr);

      // Ejemplo de parseo (ajusta según tu estructura):
      const deviceEui = dataJson.deviceInfo?.devEui || "unknown";
      const fCnt = dataJson.fCnt || 0;
      const fPort = dataJson.fPort || 0;
      const time = dataJson.time || new Date().toISOString(); // si no viene en el payload

      // Insertar en Supabase
      const { data, error } = await supabase.from(SUPABASE_TABLE).insert([
        {
          device_eui: deviceEui,
          f_cnt: fCnt,
          f_port: fPort,
          data: dataJson, // Insertamos el JSON completo
          time: time,
        },
      ]);

      if (error) {
        console.error("Error al insertar en Supabase:", error);
      } else {
        console.log("Inserción exitosa en Supabase:", data);
      }
    } catch (err) {
      console.error("Error al procesar mensaje:", err);
    }
  });

  // Manejo de errores de conexión
  client.on("error", (err) => {
    console.error("Error en la conexión MQTT:", err);
  });
}

// Llamamos a la función principal
main();
