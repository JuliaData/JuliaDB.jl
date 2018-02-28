(function() {
    document.addEventListener('DOMContentLoaded', function() {
        var unqualify = ["JuliaDB", "IndexedTables", "Dagger",
                         "DataValues", "Base.Sort", "Base"]
        var names = document.querySelectorAll(".docstring-binding code")
        console.log(names, names.length)
        for (var i=0, l=names.length; i<l; i++) {
            var txt = names[i].textContent
            for (var j=0, m=unqualify.length; j<m; j++) {
                txt = txt.replace(unqualify[j] +".", "")
            }
            console.log(txt)
            names[i].textContent = txt
        }
    })
})()
