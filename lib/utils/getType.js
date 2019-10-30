module.exports = function(type) {
  switch (type) {
    case 'char': return 'text';
    case 'float': return 'numeric';
  }
  throw 'not implemented type conversion';
};
