import { NextRequest, NextResponse } from 'next/server'
import sql from '@/lib/db';
import { RouletteEventSchema } from './schemas';

// Interfaces para los datos de la API de roulette
/*
interface RouletteOutcome {
	number: number
	type: string
	color: string
}

interface RouletteTable {
	id: string
	name: string
}

interface RouletteData {
	id: string
	startedAt: Date
	settledAt: Date
	status: string
	gameType: string
	table: RouletteTable
	result: {
		outcome: RouletteOutcome
		luckyNumbersList?: Array<{
			number: number
			roundedMultiplier: number
		}>
	}
}

interface RouletteEvent {
	id: string
	data: RouletteData
}
*/

const RouletteEventsArraySchema = RouletteEventSchema.array();

// Función para crear las tablas si no existen
async function createTables() {
  // Crear tabla casino
  await sql`
    CREATE TABLE IF NOT EXISTS casino (
      id SERIAL PRIMARY KEY,
      table_id TEXT UNIQUE NOT NULL,
      table_name TEXT NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `;

  // Crear tabla roulette_events con foreign key a casino
  await sql`
    CREATE TABLE IF NOT EXISTS roulette_events (
      id SERIAL PRIMARY KEY,
      event_id TEXT UNIQUE NOT NULL,
      started_at TIMESTAMP WITH TIME ZONE NOT NULL,
      settled_at TIMESTAMP WITH TIME ZONE NOT NULL,
      outcome_number INTEGER NOT NULL,
      outcome_type TEXT NOT NULL,
      outcome_color TEXT NOT NULL,
      casino_id INTEGER REFERENCES casino(id),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `;
}

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
    const events = await fetchRouletteData();

    const validatedFields = RouletteEventsArraySchema.parse(events);

    // Obtener la fecha settled_at más reciente de la base de datos
    const latestSettledResult = await sql`
      SELECT MAX(settled_at) as latest_settled_at FROM roulette_events
    `;
    const latestSettledAt = latestSettledResult[0]?.latest_settled_at;

    // Filtrar eventos con status "Resolved" y settled_at más reciente que el último registro
    const resolvedEvents = validatedFields.filter(event => {
      if (event.data.status !== 'Resolved') return false;

      if (!latestSettledAt) return true; // Si no hay registros previos, incluir todos

      const eventSettledAt = new Date(event.data.settledAt);
      const latestSettledDate = new Date(latestSettledAt);

      return eventSettledAt > latestSettledDate;
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

    // Obtener o crear casinos
    for (const [tableId, tableName] of uniqueTables) {      
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

        casinoCache.set(tableId, insertResult[0].id);
      } else {
        casinoCache.set(tableId, casinoResult[0].id);
      }
    }

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