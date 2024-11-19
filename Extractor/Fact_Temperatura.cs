using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extractor
{
    internal class Fact_Temperatura
    {
        public int ID_Origen { get; set; }

        public int ID_Dia { get; set; }

        public int ID_Hora { get; set; }

        public int ID_Localidad { get; set; }

        public float Temperatura { get; set; }

        public int Humedad { get; set; }

        public float SensacionTermica { get; set; }

        public float Precipitacion { get; set; }
    }
}