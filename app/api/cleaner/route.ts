import { NextRequest, NextResponse } from 'next/server'
import sql from '@/lib/db';
import { logError } from '../actions';

// Función para limpiar registros antiguos de roulette_events (>3 meses)
async function cleanupOldRouletteEvents() {
  try {
    // Calcular fecha de 3 meses atrás
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    // Eliminar registros antiguos
    const deleteResult = await sql`
      DELETE FROM roulette_events
      WHERE created_at < ${threeMonthsAgo}
    `;
    const deletedCount = deleteResult.count;

    console.log(`Limpieza completada: ${deletedCount} registros antiguos eliminados`);

    return { deleted: deletedCount };
  } catch (error) {
    console.error('Error durante la limpieza de registros antiguos:', error);

    // Registrar el error
    if (error instanceof Error) {
      await logError(error, 'cleanupOldRouletteEvents');
    }

    throw error;
  }
}

// Handler para GET requests
export async function GET(request: NextRequest) {
	try {
		// Limpiar registros antiguos
		const result = await cleanupOldRouletteEvents();

		return NextResponse.json({
			message: 'Limpieza de registros antiguos completada',
			deletedRecords: result.deleted
		});

	} catch (error) {
		console.error('Error en API de cleaner:', error)
		return NextResponse.json(
			{ error: 'Error interno del servidor' },
			{ status: 500 }
		)
	}
}