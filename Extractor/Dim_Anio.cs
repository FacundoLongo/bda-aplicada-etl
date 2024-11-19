using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Extractor
{
    internal class Dim_Anio
    {
        public int ID_Anio { get; set; }

        public int Anio { get; set; }

        public List<Dim_Mes> Meses { get; set; } = new List<Dim_Mes>();
    }
}