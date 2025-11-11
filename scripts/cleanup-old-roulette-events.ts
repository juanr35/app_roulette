import sql from '@/lib/db';
import { logError } from '@/app/api/actions'; 

// Función para limpiar registros antiguos de roulette_events (>3 meses)
async function cleanupOldRouletteEvents() {
  try {
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    const deleteResult = await sql`
      DELETE FROM roulette_events
      WHERE created_at < ${threeMonthsAgo}
    `;

    const deletedCount = deleteResult.length;

    

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

// Función principal
async function main() {
  try {
    console.log('🚀 Iniciando limpieza de registros antiguos de roulette...');

    const result = await cleanupOldRouletteEvents();

    console.log('🎉 ¡Limpieza completada exitosamente!');
    console.log(JSON.stringify(result, null, 2));

    process.exit(0);
  } catch (error) {
    console.error('💥 Error fatal:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

// Ejecutar el script
if (require.main === module) {
  main();
}