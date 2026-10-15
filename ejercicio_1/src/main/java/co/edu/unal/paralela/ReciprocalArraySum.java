package co.edu.unal.paralela;

import java.util.concurrent.RecursiveAction;

/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de
 * un arreglo usando paralelismo.
 */
public final class ReciprocalArraySum {

    /**
     * Constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Calcula secuencialmente la suma de valores recíprocos para un arreglo.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Calcula la suma de los recíprocos de los elementos del arreglo
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * calcula el tamaño de cada trozo o sección, de acuerdo con el número de
     * secciones para crear
     * a través de un número dado de elementos.
     *
     * @param nChunks   El número de secciones (chunks) para crear
     * @param nElements El número de elementos para dividir
     * @return El tamaño por defecto de la sección (chunk)
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Función techo entera
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Calcula el índice del elemento inclusivo donde la sección/trozo (chunk)
     * inicia,
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk     la sección/trozo (chunk) para cacular la posición de inicio
     * @param nChunks   Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben
     *                  atravesarse
     * @return El índice inclusivo donde esta sección/trozo (chunk) inicia en el
     *         conjunto de
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Calcula el índice del elemento exclusivo que es proporcionado al final de la
     * sección/trozo (chunk),
     * dado que hay cierto número de secciones/trozos (chunks).
     *
     * @param chunk     La sección para calcular donde termina
     * @param nChunks   Cantidad de secciones/trozos (chunks) creados
     * @param nElements La cantidad de elementos de la sección/trozo que deben
     *                  atravesarse
     * @return El índice de terminación exclusivo para esta sección/trozo (chunk)
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * Este pedazo de clase puede ser completada para para implementar el cuerpo de
     * cada tarea creada
     * para realizar la suma de los recíprocos del arreglo en paralelo.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Iniciar el índice para el recorrido transversal hecho por esta tarea.
         */
        private final int startIndexInclusive;
        /**
         * Concluir el índice para el recorrido transversal hecho por esta tarea.
         */
        private final int endIndexExclusive;
        /**
         * Arreglo de entrada para la suma de recíprocos.
         */
        private final double[] input;
        /**
         * Valor intermedio producido por esta tarea.
         */
        private double value;

        /**
         * Constructor.
         * 
         * @param setStartIndexInclusive establece el índice inicial para comenzar
         *                               el recorrido trasversal.
         * @param setEndIndexExclusive   establece el índice final para el recorrido
         *                               trasversal.
         * @param setInput               Valores de entrada
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Adquiere el valor calculado por esta tarea.
         * 
         * @return El valor calculado por esta tarea
         */
        public double getValue() {
            return value;
        }

        // para trabajar
        @Override
        protected void compute() {
            value = 0.0;

            for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                value += 1.0 / input[i];
            }
        }
    }

    /**
     * Para hacer: Modificar este método para calcular la misma suma de recíprocos
     * como le realizada en
     * seqArraySum, pero utilizando dos tareas ejecutándose en paralelo dentro del
     * framework ForkJoin de Java
     * Se puede asumir que el largo del arreglo de entrada
     * es igualmente divisible por 2.
     *
     * @param input Arreglo de entrada
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        // Calcula la mitad del arreglo
        int mid = input.length / 2;

        // Divide el arreglo en dos partes
        ReciprocalArraySumTask task1 = new ReciprocalArraySumTask(0, mid, input);
        ReciprocalArraySumTask task2 = new ReciprocalArraySumTask(mid, input.length, input);

        // Ejecuta en paralelo las tareas
        task1.fork();
        task2.compute();

        // Espera a que la primera tarea termine
        task1.join();

        // Devuelve la suma de los valores de las dos tareas
        return task1.getValue() + task2.getValue();
    }

    /**
     * Para hacer: extender el trabajo hecho para implementar parArraySum que
     * permita utilizar un número establecido
     * de tareas para calcular la suma del arreglo recíproco.
     * getChunkStartInclusive y getChunkEndExclusive pueden ser útiles para cacular
     * el rango de elementos índice que pertenecen a cada sección/trozo (chunk).
     *
     * @param input    Arreglo de entrada
     * @param numTasks El número de tareas para crear
     * @return La suma de los recíprocos del arreglo de entrada
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        double sum = 0.0;

        // Se crea un arreglo de tareas
        ReciprocalArraySumTask[] tasks = new ReciprocalArraySumTask[numTasks];

        // Se crea un bucle para crear las tareas y distribuirlas en el pool
        for (int i = 0; i < numTasks; i++) {

            // Se calcula el inicio y el fin de cada tarea
            int start = getChunkStartInclusive(i, numTasks, input.length);
            int end = getChunkEndExclusive(i, numTasks, input.length);

            // Se crea la tarea con el inicio y el fin del arreglo
            tasks[i] = new ReciprocalArraySumTask(start, end, input);
        }

        for (int i = 0; i < numTasks - 1; i++) {
            tasks[i].fork();
        }

        tasks[numTasks - 1].compute();

        for (int i = 0; i < numTasks - 1; i++) {
            tasks[i].join();
            sum += tasks[i].getValue();
        }

        sum += tasks[numTasks - 1].getValue();

        // Se devuelve la suma de los valores de las tareas
        return sum;
    }
}