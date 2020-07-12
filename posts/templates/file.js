const loadFileContent = (fieldId, callback) => {
    $(fieldId).on('change', e => {
       const file = e.target.files[0]
       const reader = new FileReader()
       reader.readAsText(file,'UTF-8')
       reader.onload = readerEvent => {
          const content = readerEvent.target.result
          callback($.parseJSON(content))
       }
    })
}