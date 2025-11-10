import { NextRequest, NextResponse } from 'next/server'
import sql from '@/lib/db';
import { createTables, logError } from '../actions';
import { RouletteEventSchema } from './schemas';

const RouletteEventsArraySchema = RouletteEventSchema.array();

// Función para obtener datos de la API de roulette
async function fetchRouletteData() {
  const response = await fetch('https://api.casinoscores.com/svc-evolution-game-events/api/megaroulette');

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();

  return data
}

// Función para procesar y almacenar los datos de roulette
async function processRouletteData() {
  try {
    // Crear tablas si no existen
    await createTables();

    // Obtener datos de la API
    //const events = await fetchRouletteData();
    const events = dataToSave

    const validatedFields = RouletteEventsArraySchema.parse(events);

    // Crear mapa de latest_settled_at por casino
    const casinoLatestSettledMap = new Map<string, Date>();

    // Obtener mesas únicas para consultar latest_settled_at por casino
    const uniqueTableIds = new Set<string>();
    validatedFields.forEach(event => {
      uniqueTableIds.add(event.data.table.id);
    });

    // Consultar latest_settled_at para cada casino
    const casinoQueries = Array.from(uniqueTableIds).map(async (tableId) => {
      const result = await sql`
        SELECT MAX(re.settled_at) as latest_settled_at
        FROM roulette_events re
        JOIN casino c ON re.casino_id = c.id
        WHERE c.table_id = ${tableId}
      `;

      return { tableId, latestSettledAt: result[0]?.latest_settled_at };
    });

    const latestSettledResults = await Promise.all(casinoQueries);

    latestSettledResults.forEach(({ tableId, latestSettledAt }) => {
      if (latestSettledAt) {
        casinoLatestSettledMap.set(tableId, new Date(latestSettledAt));
      }
    });

    // Filtrar eventos con status "Resolved" y settled_at más reciente que el último registro por casino
    const resolvedEvents = validatedFields.filter(event => {
      if (event.data.status !== 'Resolved') return false;

      const latestSettledAt = casinoLatestSettledMap.get(event.data.table.id);

      if (!latestSettledAt) return true; // Si no hay registros previos para este casino, incluir

      const eventSettledAt = new Date(event.data.settledAt);
      return eventSettledAt > latestSettledAt;
    });

    if (resolvedEvents.length === 0) {
      return { processed: 0 };
    }

    // Cache para almacenar casino_id por table_id
    const casinoCache = new Map<string, number>();

    // Preparar datos para inserción masiva
    const rouletteEventsData: Array<{
      event_id: string;
      started_at: Date;
      settled_at: Date;
      outcome_number: number;
      outcome_type: string;
      outcome_color: string;
      casino_id: number;
    }> = [];

    // Procesar casinos primero
    const uniqueTables = new Map<string, string>();

    resolvedEvents.forEach(event => {
      const { id, name } = event.data.table;

      if (!uniqueTables.has(id)) {
        uniqueTables.set(id, name);
      }
    });

    // Obtener o crear casinos en paralelo
    const casinoPromises = Array.from(uniqueTables).map(async ([tableId, tableName]) => {
      // Buscar en la base de datos
      const casinoResult = await sql`
        SELECT id
        FROM casino
        WHERE table_id = ${tableId} AND table_name = ${tableName};
      `;

      if (casinoResult.length === 0) {
        // Insertar nuevo casino
        const insertResult = await sql`
          INSERT INTO casino (table_id, table_name)
          VALUES (${tableId}, ${tableName})
          RETURNING id
        `;

        return { tableId, casinoId: insertResult[0].id };
      } else {
        return { tableId, casinoId: casinoResult[0].id };
      }
    });

    const casinoResults = await Promise.all(casinoPromises);
    casinoResults.forEach(({ tableId, casinoId }) => {
      casinoCache.set(tableId, casinoId);
    });

    // Preparar datos de eventos para inserción masiva
    resolvedEvents.forEach(event => {
      const { data } = event;
      const casinoId = casinoCache.get(data.table.id);

      if (!casinoId) {
        throw new Error("Invalid fields!")
      }

      rouletteEventsData.push({
        event_id: data.id,
        started_at: data.startedAt,
        settled_at: data.settledAt,
        outcome_number: data.result.outcome.number,
        outcome_type: data.result.outcome.type,
        outcome_color: data.result.outcome.color,
        casino_id: casinoId
      });
    });

    // Inserción masiva de eventos de roulette
    if (rouletteEventsData.length > 0) {
      await sql`
        INSERT INTO roulette_events ${
          sql(rouletteEventsData, 'event_id', 'started_at', 'settled_at', 'outcome_number', 'outcome_type', 'outcome_color', 'casino_id')
        }
        ON CONFLICT (event_id) DO NOTHING
      `;
    }

    return { processed: resolvedEvents.length };
  } catch (error) {
    console.error('Error procesando datos de roulette:', error);

    // Registrar el error en la base de datos
    if (error instanceof Error) {
      await logError(error, 'processRouletteData');
    }

    throw error;
  }
}

// Handler para GET requests
export async function GET(request: NextRequest) {
	try {
		// Procesar datos de roulette
		const result = await processRouletteData();

		return NextResponse.json({
			message: 'Datos de roulette procesados exitosamente',
			processedEvents: result.processed
		});

	} catch (error) {
		console.error('Error en API de score:', error)
		return NextResponse.json(
			{ error: 'Error interno del servidor' },
			{ status: 500 }
		)
	}
}

const dataToSave = [{"id":"69113ae63b80475fd08b5c20","data":{"id":"6aa9bf20e0d68e7558b3c517","startedAt":"2025-11-10T01:07:10Z","settledAt":"2025-11-10T01:07:50Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":35,"type":"Odd","color":"Black"},"luckyNumbersList":[{"number":0,"roundedMultiplier":100},{"number":3,"roundedMultiplier":150},{"number":4,"roundedMultiplier":100},{"number":9,"roundedMultiplier":50},{"number":27,"roundedMultiplier":50}]}}},{"id":"69113ab93b80475fd08b5c19","data":{"id":"fd24a92675a5b2e9c0991e25","startedAt":"2025-11-10T01:06:27Z","settledAt":"2025-11-10T01:07:05Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":35,"type":"Odd","color":"Black"},"luckyNumbersList":[{"number":13,"roundedMultiplier":200}]}}},{"id":"69113a8e3b80475fd08b5c12","data":{"id":"cd36ed29110eac44a8a313d3","startedAt":"2025-11-10T01:05:43Z","settledAt":"2025-11-10T01:06:22Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":2,"type":"Even","color":"Black"},"luckyNumbersList":[{"number":11,"roundedMultiplier":300},{"number":16,"roundedMultiplier":100},{"number":19,"roundedMultiplier":50}]}}},{"id":"69113a623b80475fd08b5c09","data":{"id":"3c68a41d3420657321065b67","startedAt":"2025-11-10T01:05:00Z","settledAt":"2025-11-10T01:05:38Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":5,"type":"Odd","color":"Red"},"luckyNumbersList":[{"number":9,"roundedMultiplier":100},{"number":22,"roundedMultiplier":50}]}}},{"id":"69113a373b80475fd08b5bff","data":{"id":"d51226238b67ee97229a4d6e","startedAt":"2025-11-10T01:04:17Z","settledAt":"2025-11-10T01:04:55Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":6,"type":"Even","color":"Black"},"luckyNumbersList":[{"number":11,"roundedMultiplier":100},{"number":27,"roundedMultiplier":50}]}}},{"id":"69113a0c3b80475fd08b5bf6","data":{"id":"76b25c38158f82ba23f4e075","startedAt":"2025-11-10T01:03:33Z","settledAt":"2025-11-10T01:04:12Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":0,"type":"Even","color":"Green"},"luckyNumbersList":[{"number":17,"roundedMultiplier":200},{"number":33,"roundedMultiplier":150}]}}},{"id":"691139e03b80475fd08b5bee","data":{"id":"932350b0bcec294346f56689","startedAt":"2025-11-10T01:02:51Z","settledAt":"2025-11-10T01:03:28Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":9,"type":"Odd","color":"Red"},"luckyNumbersList":[{"number":2,"roundedMultiplier":100},{"number":25,"roundedMultiplier":100},{"number":36,"roundedMultiplier":100}]}}},


{"id":"691126bcb869c09e44bca9dd","data":{"id":"18767b16ca15304333078eed","startedAt":"2025-11-09T23:40:41.972Z","settledAt":"2025-11-09T23:41:47.897Z","status":"Resolved","gameType":"roulette","currency":"EUR","table":{"id":"XxxtremeLigh0001","name":"XXXtreme Lightning Roulette"},"result":{"outcome":{"number":10,"type":"Even","color":"Black"},"luckyNumbersList":[{"number":10,"roundedMultiplier":50},{"number":21,"roundedMultiplier":700},{"number":22,"roundedMultiplier":100},{"number":23,"roundedMultiplier":100},{"number":28,"roundedMultiplier":50},{"number":33,"roundedMultiplier":100},{"number":36,"roundedMultiplier":100}],"lightningMultiplier":50}},"totalWinners":294,"totalAmount":12530,"winners":[{"screenName":"mo","winnings":1673.75},{"screenName":"Tat...","winnings":762.69},{"screenName":"Mur...","winnings":500.0},{"screenName":"Aiv...","winnings":500.0},{"screenName":"Hhj","winnings":450.0}]}
]

const dataToSave2 = [{"id":"69113a623b80475fd08b5c09","data":{"id":"3c68a41d3420657321065b67","startedAt":"2025-11-10T01:05:00Z","settledAt":"2025-11-10T01:05:38Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":5,"type":"Odd","color":"Red"},"luckyNumbersList":[{"number":9,"roundedMultiplier":100},{"number":22,"roundedMultiplier":50}]}}},{"id":"69113a373b80475fd08b5bff","data":{"id":"d51226238b67ee97229a4d6e","startedAt":"2025-11-10T01:04:17Z","settledAt":"2025-11-10T01:04:55Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":6,"type":"Even","color":"Black"},"luckyNumbersList":[{"number":11,"roundedMultiplier":100},{"number":27,"roundedMultiplier":50}]}}},{"id":"69113a0c3b80475fd08b5bf6","data":{"id":"76b25c38158f82ba23f4e075","startedAt":"2025-11-10T01:03:33Z","settledAt":"2025-11-10T01:04:12Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":0,"type":"Even","color":"Green"},"luckyNumbersList":[{"number":17,"roundedMultiplier":200},{"number":33,"roundedMultiplier":150}]}}},{"id":"691139e03b80475fd08b5bee","data":{"id":"932350b0bcec294346f56689","startedAt":"2025-11-10T01:02:51Z","settledAt":"2025-11-10T01:03:28Z","status":"Resolved","gameType":"megaroulette","table":{"id":"204","name":"Mega Roulette"},"result":{"outcome":{"number":9,"type":"Odd","color":"Red"},"luckyNumbersList":[{"number":2,"roundedMultiplier":100},{"number":25,"roundedMultiplier":100},{"number":36,"roundedMultiplier":100}]}}}]